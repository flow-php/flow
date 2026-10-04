//! Per-request state: the `Ctx` cache lives in module globals (per thread on ZTS), is built lazily and is dropped in
//! RSHUTDOWN, while the executor still runs; objects freed after RSHUTDOWN never touch it.

use ext_php_rs::prelude::PhpResult;
use ext_php_rs::zend::{ModuleGlobal, ModuleGlobals};

use crate::ctx::Ctx;
use crate::exception::ext_exception;

#[derive(Default)]
pub struct FlowGlobals {
    ctx: Option<Ctx>,
    closed: bool,
}

impl ModuleGlobal for FlowGlobals {}

pub static GLOBALS: ModuleGlobals<FlowGlobals> = ModuleGlobals::new();

pub extern "C" fn request_startup(_type: i32, _module_number: i32) -> i32 {
    unsafe { GLOBALS.get_mut() }.closed = false;

    0
}

pub extern "C" fn request_shutdown(_type: i32, _module_number: i32) -> i32 {
    let globals = unsafe { GLOBALS.get_mut() };
    globals.ctx = None;
    globals.closed = true;

    0
}

/// The only access to `Ctx`. `f` must not call PHP code: PHP re-entering flow_php would alias the `&mut Ctx`.
pub fn with_ctx<R>(f: impl FnOnce(&mut Ctx) -> PhpResult<R>) -> PhpResult<R> {
    let globals = unsafe { GLOBALS.get_mut() };

    if globals.closed {
        return Err(ext_exception("flow_php used after request shutdown"));
    }

    f(globals.ctx.get_or_insert_with(Ctx::default))
}
