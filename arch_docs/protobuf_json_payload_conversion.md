# Protobuf JSON Payload Conversion

This document records the reasoning behind the Rust SDK's Protobuf JSON payload converter and the
constraints that shaped its design. The selected direction is a descriptor-driven converter built
on `prost-reflect`, with an explicit wrapper for choosing Protobuf JSON serialization.

## The Rust-specific problems

The desired dispatch is:

```text
if the value is a protobuf message:
    use Protobuf JSON
else if the value implements serde::Serialize:
    use ordinary JSON
```

Several Rust constraints prevent the SDK from expressing this.

### Blanket implementations cannot overlap

The SDK provides ordinary JSON support with a blanket implementation for `serde::Serialize` implementers:

```rust,ignore
impl<T: serde::Serialize> TemporalSerializable for T {}
```

Adding another blanket implementation for protobuf messages would overlap for types using `pbjson` to
add Protobuf JSON serialization (or just a regular `#[derive(Serialize)]`).

Stable Rust does not provide specialization for preferring the protobuf implementation.

### Traits cannot be queried dynamically

Given an arbitrary `T`, `dyn Any`, or erased Serde value, Rust cannot ask whether it also implements
`prost::Message`. This differs from runtime interface checks used in other languages.

### `prost::Message` does not include reflection

The protobuf runtimes commonly used by other Temporal SDKs expose descriptors from every generated
message. `prost` intentionally generates structs that don't include runtime reflection or
message descriptors.

As a result, the Rust converter must obtain descriptors separately or require additional generated
code. The alternative `protobuf` runtime, for example, includes reflective `MessageFull` and
`MessageDyn` APIs.

### `serde::Serialize` has no representation metadata

Protobuf JSON compliance is a semantic property of the implementation and cannot be reliably inferred from
`serde::Serialize`. Inspecting one serialized value is also insufficient.

### Orphan rules limit downstream fixes

An application cannot implement an SDK marker trait for a protobuf type when both the trait and the
type come from other crates.

## Decision

Use `prost-reflect` and a descriptor pool as the primary Protobuf JSON implementation.

The converter:

1. Bundles descriptors for protobuf messages shipped by the SDK.
2. Accepts user-provided file descriptor sets for application messages.
3. Uses `prost::Name` to associate a typed Rust value with its fully qualified protobuf name.
4. Uses `DynamicMessage` to apply the canonical Protobuf JSON mapping.
5. Uses `ProstJsonSerializable<T>` to explicitly select Protobuf JSON for a typed value.

### Encoding

Encoding follows this path:

```text
T
  -> protobuf wire bytes using prost::Message
  -> DynamicMessage using the registered descriptor
  -> canonical Protobuf JSON
  -> Temporal Payload with encoding=json/protobuf
```

The intermediate wire representation is a cost of using prost types that do not carry reflection.
It keeps the public solution open to any prost message for which a descriptor is available.

### Decoding

Decoding follows the reverse path:

```text
Temporal Payload with encoding=json/protobuf
  -> DynamicMessage using the registered descriptor
  -> protobuf wire bytes
  -> T using prost::Message::decode
```

For typed decoding, `T::full_name()` identifies the expected schema. If `messageType` metadata is
present, it should be checked against that name. Missing `messageType` can be accepted when the
requested Rust type supplies the name; a mismatched name should produce a clear type error.

A future dynamic API may decode directly to `DynamicMessage`. Such an API would require
`messageType` metadata because there would be no requested Rust type from which to obtain the name.

## Required compatibility and safety behavior

### Unknown JSON fields must be accepted on decode

An older Rust worker must be able to receive JSON produced with a newer compatible schema.

### Encoding must not silently lose fields

The descriptor pool and Rust type are supplied through separate mechanisms, they may drift.

For example, if `T` contains a populated field added in schema version two while the pool contains
version one, `DynamicMessage` reads the wire value as an unknown field. Serializing that dynamic
message to JSON omits the field, producing a successful but incomplete payload.

Before emitting JSON, the converter must detect unknown wire fields in the dynamic message
and return a descriptor-mismatch error rather than silently dropping data.

### Descriptor failures are configuration errors

Once a value has identified itself as protobuf, a missing descriptor indicates an incomplete converter
configuration and should produce an error.

### Payload metadata selects the converter

On decode, the `encoding` metadata is authoritative.

On encode, the explicit `ProstJsonSerializable<T>` wrapper selects the representation. A plain
`serde::Serialize` value continues to use `json/plain`.

### Other SDK converter ordering is not a requirement

The default converters in several other Temporal SDKs place Protobuf JSON before protobuf binary
and plain JSON. Their ordering is significant because the same runtime protobuf value is eligible
for both protobuf converters.

That ambiguity does not exist in the Rust API with the explicit wrapper types.

## Alternatives considered

### `pbjson` as the primary converter

`pbjson-build` generates schema-specific Serde implementations that follow the Protobuf JSON
mapping. It avoids runtime reflection, descriptor/type drift, and the intermediate binary
transcoding step.

It is a good option for SDK-owned messages and applications that control their protobuf build. It
is not the primary converter because:

* Every participating Rust type must be generated with `pbjson-build`.
* No way to identify if `Serialize` implementation came from `pbjson` or `#[derive(serde::Serialize)]`.

### A Protobuf JSON marker trait

A marker cannot prove the semantics of a Serde implementation. When
combined with a wrapper it adds ceremony without improving dispatch safety.

### Requiring `prost_reflect::ReflectMessage`

`ReflectMessage` directly associates a message with its descriptor and is safer than separately
matching a type and pool by name. `prost-reflect-build` can generate the implementation.

Requiring it globally would exclude ordinary prost types and third-party generated crates. It is a
useful path for types whose generation is under the application's control, but
it does not replace descriptor-based support for foreign schemas.

### A per-type runtime registry

A registry keyed by `TypeId` can store a protobuf name and erased typed decoder for each Rust type.
Libraries such as `typetag`, `inventory`, and `prost-wkt` use related patterns for open-world trait
objects and prost messages.

This can remove wrappers at call sites, but requires registration or code generation for every
concrete Rust type and adds complexity for WASM.

### Switching to `protobuf`

The `protobuf` crate exposes reflective `MessageFull` and `MessageDyn` traits and has a dedicated
`protobuf-json-mapping` crate. Its design closely resembles the reflection available in Java, Go,
and C#.

The SDK and tonic ecosystem use prost types. Google is working on a tonic variant that works with
protobuf instead of prost: [grpc-protobuf](https://github.com/grpc/grpc-rust/tree/master/grpc-protobuf)

## Tradeoffs of the selected approach

* Descriptor sets increase binary size and runtime memory, especially for WASM builds.
* Conversion performs extra binary encoding, dynamic decoding, and allocation.
* A descriptor pool can contain only one effective version of a protobuf file.
* Users generating custom messages must emit a descriptor set and enable `prost::Name`.
* ProtoJSON itself cannot preserve unknown fields and is less evolution-friendly than protobuf
  binary encoding.
* The explicit wrapper remains visible in typed workflow and activity signatures.

## Relevant Rust libraries

* [`prost-reflect`](https://docs.rs/prost-reflect) and
  [`prost-reflect-build`](https://docs.rs/prost-reflect-build) provide runtime descriptors,
  `DynamicMessage`, and generated `ReflectMessage` implementations.
* [`pbjson`](https://docs.rs/pbjson) generates static Protobuf JSON Serde implementations.
* [`protobuf`](https://docs.rs/protobuf) and
  [`protobuf-json-mapping`](https://docs.rs/protobuf-json-mapping) demonstrate a Rust protobuf
  runtime with reflection built into generated messages.
* [`prost-wkt`](https://docs.rs/prost-wkt), [`typetag`](https://docs.rs/typetag), and
  [`inventory`](https://docs.rs/inventory) demonstrate registry-based approaches for erased or
  open-world types.
* [`erased-serde`](https://docs.rs/erased-serde) solves Serde object safety, but intentionally does
  not identify a value's concrete type or the provenance of its `Serialize` implementation.
