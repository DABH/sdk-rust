#[test]
fn interceptors_build_tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/interceptors_trybuild/*_pass.rs");
    t.compile_fail("tests/interceptors_trybuild/*_fail.rs");
}
