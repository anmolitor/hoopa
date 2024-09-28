// #[allow(non_upper_case_globals)]
// #[allow(non_camel_case_types)]
// #[allow(non_snake_case)]
// #[allow(dead_code)]
// mod bindings {
//     #[cfg(not(rust_analyzer))]
//     include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
// }
mod buf;
mod bump;
mod completion_result;
mod fd;
mod hoopa;
mod http2;
mod hyper;
mod io;
mod monoio;
mod pic;
mod picol;
mod runtime;
mod slab;
mod uninit;
mod uring_id;

fn main() -> anyhow::Result<()> {
    hoopa::main().map_err(|err| anyhow::anyhow!(err))?;
    // hyper::main().map_err(|err| anyhow::anyhow!(err))?;
    //monoio::main();
    Ok(())
}
