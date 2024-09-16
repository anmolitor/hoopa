// #[allow(non_upper_case_globals)]
// #[allow(non_camel_case_types)]
// #[allow(non_snake_case)]
// #[allow(dead_code)]
// mod bindings {
//     #[cfg(not(rust_analyzer))]
//     include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
// }
mod buf;
mod echo_server;
mod fd;
mod hoopa;
mod http2;
mod hyper;
mod naive;
mod slab;
mod uring_id;

fn main() -> anyhow::Result<()> {
    hoopa::main().map_err(|err| anyhow::anyhow!(err))?;
    //hyper::main().map_err(|err| anyhow::anyhow!(err))?;
    //naive::main();
    Ok(())
}
