//! PHP-engine plumbing: class-entry/slot lookups, hashtable helpers, calls and
//! request-scoped caches (timezones, enums, callables).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::{
    _zend_property_info, zend_call_known_function, zend_hash_index_update, zend_hash_str_find,
    zend_hash_str_update, zend_hash_update, zend_ulong,
};
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::{ClassEntry, ExecutorGlobals, Function};

use crate::exception::ext_exception;
use crate::globals::with_ctx;
use crate::plan::TypePlan;
use crate::thrift::Specs;

/// PHP array-key coercion (`_zend_handle_numeric_str`): parse + canonical
/// re-format equality rejects "+5", "-0", leading zeros and whitespace exactly
/// like the engine.
pub fn array_key_index(key: &[u8]) -> Option<i64> {
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
                zend_hash_str_update(
                    ht,
                    key.as_ptr().cast(),
                    key.len(),
                    std::ptr::from_mut(&mut value),
                );
            }
        }
    }

    std::mem::forget(value);
}

/// A PHP array key resolved once per name: numeric-string names use index
/// slots (PHP array-key coercion), everything else keys by an existing
/// zend_string so the engine reuses its cached hash instead of allocating and
/// rehashing per operation (what the `str`-based variants do).
pub enum HtKey<'a> {
    Index(i64),
    Str(&'a ZendStr),
}

/// [`ht_insert`] for a pre-resolved key; ownership of `value` moves into the
/// table.
pub fn ht_insert_key(ht: &mut ZendHashTable, key: &HtKey<'_>, mut value: Zval) {
    unsafe {
        match key {
            HtKey::Index(index) => {
                zend_hash_index_update(ht, *index as u64, std::ptr::from_mut(&mut value));
            }
            HtKey::Str(name) => {
                zend_hash_update(
                    ht,
                    std::ptr::from_ref(*name).cast_mut(),
                    std::ptr::from_mut(&mut value),
                );
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

pub fn null_zval() -> Zval {
    let mut zv = Zval::new();
    zv.set_null();
    zv
}

pub fn find_class(name: &str) -> Result<&'static ClassEntry, PhpException> {
    ClassEntry::try_find(name).ok_or_else(|| {
        ext_exception(format!(
            "flow_php requires class \"{name}\" to be autoloadable"
        ))
    })
}

/// Fails when the last engine call left an exception pending, so PHP-level
/// failures (constructor throws, decode() throws) are never silently ignored.
pub fn ensure_no_pending_exception(context: &str) -> Result<(), PhpException> {
    if ExecutorGlobals::get().exception.is_null() {
        return Ok(());
    }

    Err(ext_exception(format!("flow_php failed to {context}")))
}

/// Writes one property through the object's write handler, which copies the
/// value with assign semantics - the caller keeps ownership of `value`.
pub fn write_property_raw(
    obj: &mut ZendObject,
    name: &mut ZendStr,
    mut value: Zval,
) -> Result<(), PhpException> {
    let handler = unsafe { obj.handlers.as_ref() }
        .and_then(|handlers| handlers.write_property)
        .ok_or_else(|| ext_exception("flow_php failed to resolve a property write handler"))?;

    unsafe {
        handler(
            std::ptr::from_mut(obj),
            std::ptr::from_mut(name),
            std::ptr::from_mut(&mut value),
            std::ptr::null_mut(),
        );
    }

    ensure_no_pending_exception("write an object property")
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
                "flow_php failed to resolve property \"{name}\" on class \"{}\"",
                ce.name().unwrap_or_default()
            ))
        })?;

    Ok(unsafe { (*info).offset })
}

/// Moves an owned zval directly into a declared-property slot (what native
/// unserialize() effectively does). Only valid on a freshly created object of
/// a plain userland class when the zval matches the declared type exactly.
pub fn write_slot(obj: &mut ZendObject, offset: u32, value: Zval) {
    unsafe {
        let slot = std::ptr::from_mut(obj)
            .cast::<u8>()
            .add(offset as usize)
            .cast::<Zval>();
        std::ptr::write(slot, value);
    }
}

/// Reads one property with `BP_VAR_R` semantics; `ZendObject::get_property` passes `BP_VAR_W`, which trips the
/// readonly-property guard.
pub fn read_property(obj: &ZendObject, name: &str) -> Result<Zval, PhpException> {
    let handler = unsafe { obj.handlers.as_ref() }
        .and_then(|handlers| handlers.read_property)
        .ok_or_else(|| ext_exception("flow_php failed to resolve a property read handler"))?;

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
    .ok_or_else(|| ext_exception(format!("flow_php failed to read property \"{name}\"")))?;
    ensure_no_pending_exception(&format!("read property \"{name}\""))?;

    Ok(value.shallow_clone())
}

/// `$object->$method(...$args)`; an exception the method throws surfaces as itself.
pub fn call_method(object: &Zval, method: &str, args: &mut [Zval]) -> Result<Zval, PhpException> {
    let object = expect_object(object, "a method receiver")?;
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("flow_php failed to resolve an object class"))?;

    call_handle_transparent(ce_method_ref(ce, method)?, Some(object), args)
}

/// `$class::$method(...$args)`; an exception the method throws surfaces as itself.
pub fn call_static(class: &str, method: &str, args: &mut [Zval]) -> Result<Zval, PhpException> {
    call_handle_transparent(ce_method_ref(find_class(class)?, method)?, None, args)
}

/// Constructs a fresh object and calls its `__construct` with already-owned
/// zvals (what `construct_object` can't express - it takes `IntoZvalDyn`). Used
/// to call the canonical `Row`/`Rows` constructors from row-object graphs.
pub fn construct_with_zvals(
    ce: &'static ClassEntry,
    args: &mut [Zval],
    context: &str,
) -> Result<ZBox<ZendObject>, PhpException> {
    let mut obj = ZendObject::new(ce);
    let constructor = ce_method_ref(ce, "__construct")?;

    call_handle(
        constructor,
        Some(&mut obj),
        args,
        &format!("construct {context}"),
    )?;

    Ok(obj)
}

pub fn ce_method_ref(ce: &ClassEntry, method: &str) -> Result<&'static Function, PhpException> {
    let function = unsafe {
        ext_php_rs::ffi::zend_hash_str_find_ptr_lc(
            &raw const ce.function_table,
            method.as_ptr().cast(),
            method.len(),
        )
        .cast::<Function>()
        .as_ref()
    };

    function.ok_or_else(|| {
        ext_exception(format!(
            "flow_php failed to resolve method {}::{method}",
            ce.name().unwrap_or_default()
        ))
    })
}

/// [`call_handle`] for receivers behind a shared reference - the engine does
/// not require exclusive access to call a method.
pub fn call_handle_on(
    func: &Function,
    object: &ZendObject,
    args: &mut [Zval],
    context: &str,
) -> Result<Zval, PhpException> {
    let mut retval = Zval::new();

    unsafe {
        zend_call_known_function(
            std::ptr::from_ref(func).cast_mut(),
            std::ptr::from_ref(object).cast_mut(),
            object.ce,
            std::ptr::from_mut(&mut retval),
            args.len() as u32,
            args.as_mut_ptr(),
            std::ptr::null_mut(),
        );
    }

    ensure_no_pending_exception(context)?;

    Ok(retval)
}

/// [`call_handle`] that hands a PHP exception thrown by the callee back to the
/// caller as the exception OBJECT, so the caller can surface it verbatim or
/// replace it with one of its own. Taking it also clears the pending exception,
/// leaving the engine ready for the next call.
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

/// Surfaces a PHP exception object as ITSELF rather than wrapped in an
/// `ExtensionException`, so the class and message PHP sees are the ones thrown.
pub fn transparent_exception(exception: &mut ZendObject) -> PhpException {
    let mut zv = Zval::new();
    zv.set_object(exception);

    PhpException::default(String::new()).with_object(zv)
}

/// [`call_handle_catching`] with the exception object already re-raised as
/// itself, ready to propagate via `?` or to discard with `let Ok(x) = .. else`.
/// Callers that only need the bail branch can use [`call_handle_catching`]
/// directly instead.
pub fn call_handle_transparent(
    func: &Function,
    object: Option<&ZendObject>,
    args: &mut [Zval],
) -> Result<Zval, PhpException> {
    call_handle_catching(func, object, args)
        .map_err(|mut exception| transparent_exception(&mut exception))
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

/// Request-scoped caches of class entries, function handles and value zvals, reached only through [`with_ctx`]:
/// every function below reads and writes the cache inside the closure and calls PHP (autoload, constructors,
/// functions) outside it, so PHP code re-entering flow_php never meets a borrowed `Ctx`.
#[derive(Default)]
pub struct Ctx {
    uuid: Option<(&'static ClassEntry, u32)>,
    json: Option<(&'static ClassEntry, u32, u32)>,
    interval: Option<IntervalFns>,
    timezone: Option<TimezoneFns>,
    datetime_immutable: Option<DateTimeFns>,
    datetime_get_timezone: HashMap<usize, &'static Function>,
    datetime_cast: HashMap<usize, DateTimeCastFns>,
    timezones: HashMap<Vec<u8>, Zval>,
    enums: HashMap<Vec<u8>, Zval>,
    functions: HashMap<&'static str, &'static Function>,
    timezone_identifiers: Option<HashSet<Vec<u8>>>,
    strings: HashMap<&'static [u8], Zval>,
    instances: HashMap<&'static str, Zval>,
    plans: HashMap<Vec<u8>, Rc<TypePlan>>,
    /// `plans` by Type object handle; each entry holds its object, so the handle is not reused while cached.
    type_plans: HashMap<u32, (Zval, Rc<TypePlan>)>,
    /// The `$_TSPEC`s reachable from a root thrift class, by that class.
    thrift_specs: HashMap<String, Rc<Specs>>,
}

/// Type objects `type_plans` holds before it starts over.
const TYPE_PLANS: usize = 1024;

#[derive(Clone, Copy)]
pub struct DateTimeFns {
    pub ce: &'static ClassEntry,
    pub set_timezone: &'static Function,
}

#[derive(Clone, Copy)]
pub struct IntervalFns {
    pub ce: &'static ClassEntry,
    pub construct: &'static Function,
}

#[derive(Clone, Copy)]
pub struct TimezoneFns {
    pub ce: &'static ClassEntry,
    pub construct: &'static Function,
    pub get_name: &'static Function,
}

#[derive(Clone, Copy)]
pub struct DateTimeCastFns {
    pub set_time: &'static Function,
    pub format: &'static Function,
}

/// One instance of a stateless PHP class (a `Physical`, `PhysicalFor`), constructed without arguments.
pub fn instance(class: &'static str) -> Result<Zval, PhpException> {
    cached(
        |ctx| ctx.instances.get(class).map(Zval::shallow_clone),
        || {
            let ce = find_class(class)?;
            let mut object = ZendObject::new(ce);

            if !ce.constructor.is_null() {
                call_handle_transparent(ce_method_ref(ce, "__construct")?, Some(&object), &mut [])?;
            }

            let mut zv = Zval::new();
            zv.set_object(&mut object);

            Ok(zv)
        },
        |ctx, zv| {
            ctx.instances.insert(class, zv.shallow_clone());
        },
    )
}

pub fn plan(json: &[u8]) -> Result<Option<Rc<TypePlan>>, PhpException> {
    with_ctx(|ctx| Ok(ctx.plans.get(json).cloned()))
}

pub fn store_plan(json: Vec<u8>, plan: &Rc<TypePlan>) -> Result<(), PhpException> {
    with_ctx(|ctx| {
        ctx.plans.insert(json, Rc::clone(plan));
        Ok(())
    })
}

/// The plan of a Type object seen before; Types are immutable, so the object names its plan.
pub fn type_plan_of(type_zv: &Zval) -> Result<Option<Rc<TypePlan>>, PhpException> {
    let Some(handle) = type_zv.object().map(|object| object.handle) else {
        return Ok(None);
    };

    with_ctx(|ctx| Ok(ctx.type_plans.get(&handle).map(|(_, plan)| Rc::clone(plan))))
}

pub fn store_type_plan(type_zv: &Zval, plan: &Rc<TypePlan>) -> Result<(), PhpException> {
    let Some(handle) = type_zv.object().map(|object| object.handle) else {
        return Ok(());
    };

    // the evicted objects are released outside the context: freeing one may run PHP code
    let _evicted = with_ctx(|ctx| {
        let evicted = if ctx.type_plans.len() >= TYPE_PLANS {
            std::mem::take(&mut ctx.type_plans)
        } else {
            HashMap::new()
        };
        ctx.type_plans.insert(handle, (type_zv.shallow_clone(), Rc::clone(plan)));

        Ok(evicted)
    })?;

    Ok(())
}

pub fn thrift_specs(class: &str) -> Result<Option<Rc<Specs>>, PhpException> {
    with_ctx(|ctx| Ok(ctx.thrift_specs.get(class).cloned()))
}

pub fn store_thrift_specs(class: &str, specs: &Rc<Specs>) -> Result<(), PhpException> {
    with_ctx(|ctx| {
        ctx.thrift_specs.insert(class.to_string(), Rc::clone(specs));
        Ok(())
    })
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

/// An internal PHP function; the engine's function table outlives every request.
fn function(name: &'static str) -> Result<&'static Function, PhpException> {
    cached(
        |ctx| ctx.functions.get(name).copied(),
        || {
            let table = ExecutorGlobals::get().function_table;

            let found = unsafe { ext_php_rs::ffi::zend_hash_str_find_ptr_lc(table, name.as_ptr().cast(), name.len()) }
                .cast::<Function>();

            unsafe { found.as_ref() }
                .ok_or_else(|| ext_exception(format!("flow_php failed to resolve function {name}")))
        },
        |ctx, function| {
            ctx.functions.insert(name, function);
        },
    )
}

pub fn json_encode() -> Result<&'static Function, PhpException> {
    function("json_encode")
}

pub fn json_validate() -> Result<&'static Function, PhpException> {
    function("json_validate")
}

pub fn date_parse() -> Result<&'static Function, PhpException> {
    function("date_parse")
}

/// A cached string zval, e.g. a format string.
pub fn string(bytes: &'static [u8]) -> Result<Zval, PhpException> {
    cached(
        |ctx| ctx.strings.get(bytes).map(Zval::shallow_clone),
        || Ok(zval_str(bytes)),
        |ctx, zv| {
            ctx.strings.insert(bytes, zv.shallow_clone());
        },
    )
}

/// `"true"`/`"false"` for bool-to-string casts.
pub fn bool_str(value: bool) -> Result<Zval, PhpException> {
    string(if value { b"true" } else { b"false" })
}

pub fn uuid() -> Result<(&'static ClassEntry, u32), PhpException> {
    cached(
        |ctx| ctx.uuid,
        || {
            let ce = find_class("Flow\\Types\\Value\\Uuid")?;
            Ok((ce, property_offset(ce, "value")?))
        },
        |ctx, uuid| ctx.uuid = Some(*uuid),
    )
}

pub fn json() -> Result<(&'static ClassEntry, u32, u32), PhpException> {
    cached(
        |ctx| ctx.json,
        || {
            let ce = find_class("Flow\\Types\\Value\\Json")?;
            Ok((ce, property_offset(ce, "value")?, property_offset(ce, "isObject")?))
        },
        |ctx, json| ctx.json = Some(*json),
    )
}

pub fn interval() -> Result<IntervalFns, PhpException> {
    cached(
        |ctx| ctx.interval,
        || {
            let ce = find_class("DateInterval")?;
            Ok(IntervalFns {
                ce,
                construct: ce_method_ref(ce, "__construct")?,
            })
        },
        |ctx, interval| ctx.interval = Some(*interval),
    )
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
                get_name: ce_method_ref(ce, "getName")?,
            })
        },
        |ctx, fns| ctx.timezone = Some(*fns),
    )
}

pub fn timezone_get_name() -> Result<&'static Function, PhpException> {
    Ok(timezone_fns()?.get_name)
}

/// `getTimezone` of a datetime class, subclass-safe.
pub fn datetime_get_timezone(ce: &'static ClassEntry) -> Result<&'static Function, PhpException> {
    let key = std::ptr::from_ref(ce) as usize;

    cached(
        |ctx| ctx.datetime_get_timezone.get(&key).copied(),
        || ce_method_ref(ce, "getTimezone"),
        |ctx, function| {
            ctx.datetime_get_timezone.insert(key, function);
        },
    )
}

/// `setTime`/`format` of a datetime class, subclass-safe.
pub fn datetime_cast_fns(ce: &'static ClassEntry) -> Result<DateTimeCastFns, PhpException> {
    let key = std::ptr::from_ref(ce) as usize;

    cached(
        |ctx| ctx.datetime_cast.get(&key).copied(),
        || {
            Ok(DateTimeCastFns {
                set_time: ce_method_ref(ce, "setTime")?,
                format: ce_method_ref(ce, "format")?,
            })
        },
        |ctx, fns| {
            ctx.datetime_cast.insert(key, *fns);
        },
    )
}

/// `new DateTimeZone($name)`, one instance per name.
pub fn timezone(name: &[u8]) -> Result<Zval, PhpException> {
    cached(
        |ctx| ctx.timezones.get(name).map(Zval::shallow_clone),
        || {
            let fns = timezone_fns()?;
            let mut timezone = ZendObject::new(fns.ce);
            call_handle(fns.construct, Some(&mut timezone), &mut [zval_str(name)], "construct DateTimeZone")?;

            let mut zv = Zval::new();
            zv.set_object(&mut timezone);

            Ok(zv)
        },
        |ctx, zv| {
            ctx.timezones.insert(name.to_vec(), zv.shallow_clone());
        },
    )
}

/// Whether `new DateTimeZone($name)` succeeds; the exception it throws otherwise is discarded.
pub fn timezone_accepts(name: &[u8]) -> Result<bool, PhpException> {
    let fns = timezone_fns()?;
    let timezone = ZendObject::new(fns.ce);

    Ok(call_handle_catching(fns.construct, Some(&timezone), &mut [zval_str(name)]).is_ok())
}

/// Whether `$name` is in `DateTimeZone::listIdentifiers()`, fetched once per request.
pub fn is_timezone_identifier(name: &[u8]) -> Result<bool, PhpException> {
    if let Some(known) = with_ctx(|ctx| Ok(ctx.timezone_identifiers.as_ref().map(|ids| ids.contains(name))))? {
        return Ok(known);
    }

    let list = call_handle(
        ce_method_ref(timezone_fns()?.ce, "listIdentifiers")?,
        None,
        &mut [],
        "list timezone identifiers",
    )?;
    let identifiers: HashSet<Vec<u8>> = list
        .array()
        .ok_or_else(|| ext_exception("flow_php expected DateTimeZone::listIdentifiers() to return an array"))?
        .values()
        .filter_map(|zv| zv.zend_str().map(|name| name.as_bytes().to_vec()))
        .collect();
    let known = identifiers.contains(name);

    with_ctx(|ctx| {
        ctx.timezone_identifiers = Some(identifiers);
        Ok(())
    })?;

    Ok(known)
}

/// An enum case, cached per `Class::Case`, resolved with `enum_exists` + `defined` + `constant`.
pub fn enum_case(class: &[u8], case: &[u8]) -> Result<Zval, PhpException> {
    let mut constant_name = Vec::with_capacity(class.len() + case.len() + 2);
    constant_name.extend_from_slice(class);
    constant_name.extend_from_slice(b"::");
    constant_name.extend_from_slice(case);

    cached(
        |ctx| ctx.enums.get(&constant_name).map(Zval::shallow_clone),
        || {
            let class_name = String::from_utf8_lossy(class);
            let case_name = String::from_utf8_lossy(case);
            let is_enum = std::str::from_utf8(class)
                .ok()
                .and_then(ClassEntry::try_find)
                .is_some_and(|ce| ce.flags().contains(ClassFlags::Enum));

            if !is_enum {
                return Err(ext_exception(format!(
                    "flow_php cannot restore enum of class \"{class_name}\", enum not found"
                )));
            }

            let constant_name_zv = zval_str(&constant_name);
            let defined = call_handle(
                function("defined")?,
                None,
                &mut [constant_name_zv.shallow_clone()],
                "check an enum case",
            )?;
            let case_zv = if defined.bool().unwrap_or(false) {
                call_handle(function("constant")?, None, &mut [constant_name_zv], "restore an enum case")?
            } else {
                null_zval()
            };

            if !case_zv.is_object() {
                return Err(ext_exception(format!(
                    "flow_php cannot restore enum case \"{class_name}::{case_name}\""
                )));
            }

            Ok(case_zv)
        },
        |ctx, zv| {
            ctx.enums.insert(constant_name.clone(), zv.shallow_clone());
        },
    )
}

/// `HASH_FLAG_PACKED` from zend_types.h.
pub const HASH_FLAG_PACKED: u8 = 1 << 2;

/// Raw order-preserving iteration over packed and bucket layouts, binary-safe
/// for arbitrary keys (ext-php-rs's `iter()` panics on non-UTF-8 string keys).
pub fn ht_for_each(
    ht: &ZendHashTable,
    mut f: impl FnMut(Option<&ZendStr>, zend_ulong, &Zval) -> Result<(), PhpException>,
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

pub fn read_slot(obj: &ZendObject, offset: u32) -> &Zval {
    unsafe {
        &*std::ptr::from_ref(obj)
            .cast::<u8>()
            .add(offset as usize)
            .cast::<Zval>()
    }
}

pub fn expect_object<'a>(
    zv: &'a Zval,
    context: &str,
) -> Result<&'a ZendObject, PhpException> {
    zv.object()
        .ok_or_else(|| ext_exception(format!("flow_php expected {context} to be an object")))
}
