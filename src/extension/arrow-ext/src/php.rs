//! PHP-engine plumbing: class-entry/slot lookups, hashtable helpers, calls, and the request-scoped cache of datetime
//! handles, timezones and thrift specs.

use std::collections::HashMap;
use std::rc::Rc;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::FromZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::{
    _zend_property_info, zend_call_known_function, zend_hash_index_update, zend_hash_str_find, zend_hash_str_update,
    zend_ulong,
};
use ext_php_rs::flags::DataType;
use ext_php_rs::prelude::PhpResult;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::{ce, ClassEntry, ExecutorGlobals, Function, ModuleGlobal, ModuleGlobals};

use crate::exception::ext_exception;
use crate::thrift::Specs;

/// PHP array-key coercion (`_zend_handle_numeric_str`): parse + canonical
/// re-format equality rejects "+5", "-0", leading zeros and whitespace exactly
/// like the engine.
fn array_key_index(key: &[u8]) -> Option<i64> {
    let string = std::str::from_utf8(key).ok()?;
    let value: i64 = string.parse().ok()?;

    (value.to_string().as_bytes() == key).then_some(value)
}

/// Inserts with PHP array-assignment semantics; ownership of `value` moves
/// into the table.
pub fn ht_insert(ht: &mut ZendHashTable, key: &[u8], mut value: Zval) {
    unsafe {
        match array_key_index(key) {
            Some(index) => {
                zend_hash_index_update(ht, index as u64, std::ptr::from_mut(&mut value));
            }
            None => {
                zend_hash_str_update(ht, key.as_ptr().cast(), key.len(), std::ptr::from_mut(&mut value));
            }
        }
    }

    std::mem::forget(value);
}

pub fn ht_insert_long(ht: &mut ZendHashTable, index: i64, mut value: Zval) {
    unsafe {
        zend_hash_index_update(ht, index as u64, std::ptr::from_mut(&mut value));
    }

    std::mem::forget(value);
}

/// `$array[$key]` with PHP array-key coercion.
pub fn ht_get<'a>(ht: &'a ZendHashTable, key: &[u8]) -> Option<&'a Zval> {
    match array_key_index(key) {
        Some(index) => ht.get_index(index),
        None => unsafe { zend_hash_str_find(ht, key.as_ptr().cast(), key.len()).as_ref() },
    }
}

pub fn zval_str(bytes: &[u8]) -> Zval {
    let mut zv = Zval::new();
    zv.set_zend_string(ZendStr::new(bytes, false));
    zv
}

pub fn zval_long(value: i64) -> Zval {
    let mut zv = Zval::new();
    zv.set_long(value);
    zv
}

pub fn find_class(name: &str) -> Result<&'static ClassEntry, PhpException> {
    ClassEntry::try_find(name)
        .ok_or_else(|| ext_exception(format!("arrow requires class \"{name}\" to be autoloadable")))
}

/// Fails when the last engine call left an exception pending, so PHP-level
/// failures are never silently ignored.
pub fn ensure_no_pending_exception(context: &str) -> Result<(), PhpException> {
    if ExecutorGlobals::get().exception.is_null() {
        return Ok(());
    }

    Err(ext_exception(format!("arrow failed to {context}")))
}

/// Resolves a declared property's property-table slot offset; keyed by the
/// plain (unmangled) name for private properties too.
pub fn property_offset(ce: &ClassEntry, name: &str) -> Result<u32, PhpException> {
    let properties_info: &ZendHashTable = &ce.properties_info;

    let info = properties_info
        .get(name)
        .and_then(|zv| unsafe { zv.ptr::<_zend_property_info>() })
        .ok_or_else(|| {
            ext_exception(format!(
                "arrow failed to resolve property \"{name}\" on class \"{}\"",
                ce.name().unwrap_or_default()
            ))
        })?;

    Ok(unsafe { (*info).offset })
}

/// Reads one property with `BP_VAR_R` semantics; `ZendObject::get_property` passes `BP_VAR_W`, which trips the
/// readonly-property guard.
pub fn read_property(obj: &ZendObject, name: &str) -> Result<Zval, PhpException> {
    let handler = unsafe { obj.handlers.as_ref() }
        .and_then(|handlers| handlers.read_property)
        .ok_or_else(|| ext_exception("arrow failed to resolve a property read handler"))?;

    let mut name_zstr = ZendStr::new(name, false);
    let mut rv = Zval::new();

    let value = unsafe {
        handler(
            std::ptr::from_ref(obj).cast_mut(),
            std::ptr::from_mut(&mut *name_zstr),
            0,
            std::ptr::null_mut(),
            std::ptr::from_mut(&mut rv),
        )
        .as_ref()
    }
    .ok_or_else(|| ext_exception(format!("arrow failed to read property \"{name}\"")))?;
    ensure_no_pending_exception(&format!("read property \"{name}\""))?;

    Ok(value.shallow_clone())
}

pub fn ce_method_ref(ce: &ClassEntry, method: &str) -> Result<&'static Function, PhpException> {
    let function = unsafe {
        ext_php_rs::ffi::zend_hash_str_find_ptr_lc(&raw const ce.function_table, method.as_ptr().cast(), method.len())
            .cast::<Function>()
            .as_ref()
    };

    function.ok_or_else(|| {
        ext_exception(format!(
            "arrow failed to resolve method {}::{method}",
            ce.name().unwrap_or_default()
        ))
    })
}

/// `$object->$method(...$args)`; an exception the method throws surfaces as itself.
pub fn call_method(object: &Zval, method: &str, args: &mut [Zval]) -> Result<Zval, PhpException> {
    let object = expect_object(object, "a method receiver")?;
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("arrow failed to resolve an object class"))?;

    call_handle_transparent(ce_method_ref(ce, method)?, Some(object), args)
}

/// Calls a pre-resolved function handle and hands a PHP exception thrown by the callee back to the caller as the
/// exception OBJECT, so the caller can surface it verbatim or replace it with one of its own. Taking it also clears
/// the pending exception, leaving the engine ready for the next call.
pub fn call_handle_catching(
    func: &Function,
    object: Option<&ZendObject>,
    args: &mut [Zval],
) -> Result<Zval, ZBox<ZendObject>> {
    let mut retval = Zval::new();

    let (object_ptr, called_scope) = match object {
        Some(obj) => (std::ptr::from_ref(obj).cast_mut(), obj.ce),
        None => (std::ptr::null_mut(), unsafe { func.common.scope }),
    };

    unsafe {
        zend_call_known_function(
            std::ptr::from_ref(func).cast_mut(),
            object_ptr,
            called_scope,
            std::ptr::from_mut(&mut retval),
            args.len() as u32,
            args.as_mut_ptr(),
            std::ptr::null_mut(),
        );
    }

    match ExecutorGlobals::take_exception() {
        Some(exception) => Err(exception),
        None => Ok(retval),
    }
}

/// Surfaces a PHP exception object as ITSELF rather than wrapped, so the class and message PHP sees are the ones
/// thrown.
pub fn transparent_exception(exception: &mut ZendObject) -> PhpException {
    let mut zv = Zval::new();
    zv.set_object(exception);

    PhpException::from_message(String::new()).with_object(zv)
}

/// [`call_handle_catching`] with the exception object already re-raised as itself.
pub fn call_handle_transparent(
    func: &Function,
    object: Option<&ZendObject>,
    args: &mut [Zval],
) -> Result<Zval, PhpException> {
    call_handle_catching(func, object, args).map_err(|mut exception| transparent_exception(&mut exception))
}

/// Calls a pre-resolved function handle. Argument zvals stay caller-owned
/// (the engine copies what it keeps), so cached zvals pass as shallow clones.
pub fn call_handle(
    func: &Function,
    object: Option<&mut ZendObject>,
    args: &mut [Zval],
    context: &str,
) -> Result<Zval, PhpException> {
    let mut retval = Zval::new();

    let (object_ptr, called_scope) = match object {
        Some(obj) => {
            let scope = obj.ce;
            (std::ptr::from_mut(obj), scope)
        }
        None => (std::ptr::null_mut(), unsafe { func.common.scope }),
    };

    unsafe {
        zend_call_known_function(
            std::ptr::from_ref(func).cast_mut(),
            object_ptr,
            called_scope,
            std::ptr::from_mut(&mut retval),
            args.len() as u32,
            args.as_mut_ptr(),
            std::ptr::null_mut(),
        );
    }

    ensure_no_pending_exception(context)?;

    Ok(retval)
}

/// `HASH_FLAG_PACKED` from zend_types.h.
const HASH_FLAG_PACKED: u8 = 1 << 2;

/// Raw order-preserving iteration over packed and bucket layouts, binary-safe
/// for arbitrary keys (ext-php-rs's `iter()` panics on non-UTF-8 string keys).
pub fn ht_for_each<'a>(
    ht: &'a ZendHashTable,
    mut f: impl FnMut(Option<&'a ZendStr>, zend_ulong, &'a Zval) -> Result<(), PhpException>,
) -> Result<(), PhpException> {
    let packed = unsafe { ht.u.v.flags } & HASH_FLAG_PACKED != 0;

    for index in 0..ht.nNumUsed {
        if packed {
            let value = unsafe { &*ht.__bindgen_anon_1.arPacked.add(index as usize) };

            if value.get_type() == ext_php_rs::flags::DataType::Undef {
                continue;
            }

            f(None, zend_ulong::from(index), value)?;
        } else {
            let bucket = unsafe { &*ht.__bindgen_anon_1.arData.add(index as usize) };

            if bucket.val.get_type() == ext_php_rs::flags::DataType::Undef {
                continue;
            }

            let key = unsafe { bucket.key.as_ref() };
            f(key, bucket.h, &bucket.val)?;
        }
    }

    Ok(())
}

pub fn expect_object<'a>(zv: &'a Zval, context: &str) -> Result<&'a ZendObject, PhpException> {
    zv.object()
        .ok_or_else(|| ext_exception(format!("arrow expected {context} to be an object")))
}

/// Request-scoped caches of class entries, function handles and value zvals, reached only through [`with_ctx`]:
/// every function below reads and writes the cache inside the closure and calls PHP (autoload, constructors)
/// outside it, so PHP code re-entering arrow never meets a borrowed `Ctx`.
#[derive(Default)]
pub struct Ctx {
    timezone: Option<TimezoneFns>,
    datetime_immutable: Option<DateTimeFns>,
    timezones: HashMap<Vec<u8>, Zval>,
    /// The `$_TSPEC`s reachable from a root thrift class, by that class.
    pub thrift_specs: HashMap<String, Rc<Specs>>,
}

#[derive(Clone, Copy)]
pub struct DateTimeFns {
    pub ce: &'static ClassEntry,
    pub set_timezone: &'static Function,
}

#[derive(Clone, Copy)]
struct TimezoneFns {
    ce: &'static ClassEntry,
    construct: &'static Function,
}

/// A cached handle: read inside the context, resolved outside it on a miss, then stored.
fn cached<T>(
    read: impl FnOnce(&Ctx) -> Option<T>,
    resolve: impl FnOnce() -> Result<T, PhpException>,
    store: impl FnOnce(&mut Ctx, &T),
) -> Result<T, PhpException> {
    if let Some(value) = with_ctx(|ctx| Ok(read(ctx)))? {
        return Ok(value);
    }

    let value = resolve()?;
    with_ctx(|ctx| {
        store(ctx, &value);
        Ok(())
    })?;

    Ok(value)
}

pub fn datetime_immutable() -> Result<DateTimeFns, PhpException> {
    cached(
        |ctx| ctx.datetime_immutable,
        || {
            let ce = find_class("DateTimeImmutable")?;
            Ok(DateTimeFns {
                ce,
                set_timezone: ce_method_ref(ce, "setTimezone")?,
            })
        },
        |ctx, fns| ctx.datetime_immutable = Some(*fns),
    )
}

fn timezone_fns() -> Result<TimezoneFns, PhpException> {
    cached(
        |ctx| ctx.timezone,
        || {
            let ce = find_class("DateTimeZone")?;
            Ok(TimezoneFns {
                ce,
                construct: ce_method_ref(ce, "__construct")?,
            })
        },
        |ctx, fns| ctx.timezone = Some(*fns),
    )
}

/// `new DateTimeZone($name)`, one instance per name.
pub fn timezone(name: &[u8]) -> Result<Zval, PhpException> {
    cached(
        |ctx| ctx.timezones.get(name).map(Zval::shallow_clone),
        || {
            let fns = timezone_fns()?;
            let mut timezone = ZendObject::new(fns.ce);
            call_handle(
                fns.construct,
                Some(&mut timezone),
                &mut [zval_str(name)],
                "construct DateTimeZone",
            )?;

            let mut zv = Zval::new();
            zv.set_object(&mut timezone);

            Ok(zv)
        },
        |ctx, zv| {
            ctx.timezones.insert(name.to_vec(), zv.shallow_clone());
        },
    )
}

/// Per-request state: the `Ctx` cache lives in module globals (per thread on ZTS), is built lazily and is dropped in
/// RSHUTDOWN, while the executor still runs; objects freed after RSHUTDOWN never touch it.
#[derive(Default)]
pub struct ArrowGlobals {
    ctx: Option<Ctx>,
    closed: bool,
}

impl ModuleGlobal for ArrowGlobals {}

pub static GLOBALS: ModuleGlobals<ArrowGlobals> = ModuleGlobals::new();

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

/// The only access to `Ctx`. `f` must not call PHP code: PHP re-entering arrow would alias the `&mut Ctx`.
pub fn with_ctx<R>(f: impl FnOnce(&mut Ctx) -> PhpResult<R>) -> PhpResult<R> {
    let globals = unsafe { GLOBALS.get_mut() };

    if globals.closed {
        return Err(ext_exception("arrow used after request shutdown"));
    }

    f(globals.ctx.get_or_insert_with(Ctx::default))
}

/// An `iterable` argument kept as its zval. ext-php-rs's `Iterable` opens a `zend_object_iterator` for a `Traversable`
/// and 0.16 never frees it (no `zend_iterator_dtor`): the iterator and its generator lived until the request ended.
pub struct IterableArg<'a>(pub &'a Zval);

impl<'a> FromZval<'a> for IterableArg<'a> {
    const TYPE: DataType = DataType::Iterable;

    fn from_zval(zval: &'a Zval) -> Option<Self> {
        (zval.is_array() || zval.is_traversable()).then_some(Self(zval))
    }
}

/// Every value of `$iterable` in order, keys ignored: an `Iterator` through `rewind()` / `valid()` / `current()` /
/// `next()` as `foreach` does, an `IteratorAggregate` through `getIterator()`.
pub fn for_each_value(iterable: &Zval, mut f: impl FnMut(&Zval) -> PhpResult<()>) -> PhpResult<()> {
    if let Some(array) = iterable.array() {
        for value in array.values() {
            f(value)?;
        }

        return Ok(());
    }

    let mut iterator = iterable.shallow_clone();

    while iterator
        .object()
        .is_some_and(|object| object.instance_of(ce::aggregate()))
    {
        iterator = call_method(&iterator, "getIterator", &mut [])?;
    }

    if !iterator
        .object()
        .is_some_and(|object| object.instance_of(ce::iterator()))
    {
        return Err(ext_exception("arrow expected getIterator() to return a Traversable"));
    }

    call_method(&iterator, "rewind", &mut [])?;

    while call_method(&iterator, "valid", &mut [])?.bool().unwrap_or(false) {
        f(&call_method(&iterator, "current", &mut [])?)?;
        call_method(&iterator, "next", &mut [])?;
    }

    Ok(())
}
