use ffkv::client;
#[path = "../mcp/mod.rs"]
mod mcp;

use clap::Parser;

fn main() {
    let config = match mcp::config::Config::load(mcp::config::Options::parse()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("ffkv-mcp: {error}");
            std::process::exit(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("ffkv-mcp: could not initialize asynchronous runtime");
            std::process::exit(2);
        }
    };
    let result = runtime.block_on(mcp::run(config));
    // Tokio's stdin reader can be blocked in an OS read after a signal. All owned
    // network sessions and child processes have already been cleaned up by run().
    runtime.shutdown_background();
    if let Err(error) = result {
        eprintln!("ffkv-mcp: {error}");
        std::process::exit(1);
    }
}
