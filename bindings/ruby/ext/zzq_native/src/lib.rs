//! Native part of the `zzq` gem: `ZZQ::Native`, which `lib/zzq` wraps.

mod broker;
mod client;
mod context;
mod error;
mod op;
mod options;
mod rb;
mod wake;

use rb_sys::VALUE;

use crate::rb::RbResult;

unsafe extern "C" fn io_threads(_module: VALUE) -> VALUE {
    rb::wrap(|| Ok(rb::u64_value(context::io_threads() as u64)))
}

unsafe extern "C" fn set_io_threads(_module: VALUE, n: VALUE) -> VALUE {
    rb::wrap(|| {
        let n = usize::try_from(rb::value_to_i64(n)?)
            .map_err(|_| rb::RubyErr::arg("io_threads must be positive"))?;
        if !context::set_io_threads(n) {
            return Err(error::zzq(
                "set io_threads before the first client or broker",
            ));
        }
        Ok(rb::qnil())
    })
}

/// Called once by Ruby when it loads the extension.
#[unsafe(no_mangle)]
pub extern "C" fn Init_zzq_native() {
    if let Err(e) = init() {
        rb::raise(e);
    }
}

fn init() -> RbResult<()> {
    let zzq = rb::define_module(c"ZZQ")?;
    let native = rb::define_module_under(zzq, c"Native")?;
    error::init(zzq)?;
    rb::define_singleton_method(native, c"io_threads", io_threads as *const (), 0)?;
    rb::define_singleton_method(native, c"io_threads=", set_io_threads as *const (), 1)?;
    wake::register(native)?;
    op::register(native)?;
    client::register(zzq, native)?;
    broker::register(native)?;
    Ok(())
}
