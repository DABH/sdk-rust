use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Attribute, Ident, ItemFn, LitStr, Token,
    parse::{Parse, ParseStream},
};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    syn::parse2::<CloudTestExclusionArgs>(attr)?;
    let test_fn = syn::parse2::<ItemFn>(item)?;
    if !has_test_attribute(&test_fn.attrs) {
        return Err(syn::Error::new_spanned(
            &test_fn.sig.ident,
            "cloud_test_exclusion can only be applied to a test function",
        ));
    }

    Ok(gate_item(quote!(#test_fn)))
}

pub(crate) fn expand_module(input: TokenStream) -> syn::Result<TokenStream> {
    let input = syn::parse2::<CloudTestModuleExclusion>(input)?;
    let module = input.module;
    Ok(gate_item(quote!(mod #module;)))
}

fn gate_item(item: TokenStream) -> TokenStream {
    quote! {
        #[cfg(any(not(feature = "cloud-test-mode"), clippy))]
        #item
    }
}

struct CloudTestExclusionArgs;

impl Parse for CloudTestExclusionArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        parse_reason(input)?;
        parse_optional_note(input)?;
        Ok(Self)
    }
}

struct CloudTestModuleExclusion {
    module: Ident,
}

impl Parse for CloudTestModuleExclusion {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        parse_reason(input)?;
        input.parse::<Token![,]>()?;
        let module = input.parse::<Ident>()?;
        parse_optional_note(input)?;
        Ok(Self { module })
    }
}

fn parse_reason(input: ParseStream<'_>) -> syn::Result<()> {
    let reason = input.parse::<Ident>()?;
    match reason.to_string().as_str() {
        "RequiresLocalServer" | "RequiresCloudProvisioning" | "NeedsCloudAdaptation" => Ok(()),
        _ => Err(syn::Error::new_spanned(
            reason,
            "unknown Cloud test exclusion reason",
        )),
    }
}

fn parse_optional_note(input: ParseStream<'_>) -> syn::Result<()> {
    if input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        if !input.is_empty() {
            let note = input.parse::<LitStr>()?;
            if note.value().trim().is_empty() {
                return Err(syn::Error::new_spanned(
                    note,
                    "exclusion note cannot be empty",
                ));
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
    }
    if !input.is_empty() {
        return Err(input.error("unexpected cloud test exclusion argument"));
    }
    Ok(())
}

fn has_test_attribute(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        has_attribute_path(attribute, &["test"])
            || has_attribute_path(attribute, &["tokio", "test"])
            || has_attribute_path(attribute, &["rstest"])
            || has_attribute_path(attribute, &["rstest", "rstest"])
    })
}

fn has_attribute_path(attribute: &Attribute, expected: &[&str]) -> bool {
    attribute.path().segments.len() == expected.len()
        && attribute
            .path()
            .segments
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual.ident == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn gates_module_with_optional_note() {
        let expanded = expand_module(quote!(
            RequiresLocalServer,
            example,
            "Starts a local server."
        ))
        .unwrap()
        .to_string();

        assert!(expanded.contains("mod example"));
        assert!(expanded.contains("cloud-test-mode"));
        assert!(expanded.contains("clippy"));
    }

    #[test]
    fn gates_rstest_template() {
        expand(
            quote!(NeedsCloudAdaptation),
            quote! {
                #[rstest::rstest]
                #[case(true)]
                #[case(false)]
                #[tokio::test]
                async fn example(#[case] value: bool) {}
            },
        )
        .unwrap();
    }

    #[test]
    fn rejects_unknown_reason() {
        let error = expand_module(quote!(Unknown, example)).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown Cloud test exclusion reason")
        );
    }
}
