// #[allow(non_upper_case_globals)]
// #[allow(non_camel_case_types)]
// #[allow(non_snake_case)]
// #[allow(dead_code)]
// mod bindings {
//     #[cfg(not(rust_analyzer))]
//     include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
// }
mod echo_server;
mod hyper;
//mod iouring;

fn main() -> anyhow::Result<()> {
    echo_server::main().map_err(|err| anyhow::anyhow!(err))?;
    Ok(())
}
