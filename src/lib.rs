pub mod bindings {
    wit_bindgen_wrpc::generate!({
        world: "interfaces",
        generate_all,
    });
}
