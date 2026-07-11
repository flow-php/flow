//! PHP-engine plumbing: class-entry/slot lookups, hashtable helpers, calls and
//! per-instance caches (timezones, enums, callables).

use std::collections::HashMap;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZvalDyn;
use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::{
    _zend_property_info, zend_call_known_function, zend_hash_index_update, zend_hash_str_find,
    zend_hash_str_update, zend_objects_clone_members, zend_ulong,
};
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::{ClassEntry, ExecutorGlobals, Function};

use crate::exception::ext_exception;

// Not in ext-php-rs's allowed bindings; ZEND_API symbols with the same ABI as
// the bound zend_hash_*_update functions.
extern "C" {
    fn zend_hash_str_add(
        ht: *mut ZendHashTable,
        key: *const std::ffi::c_char,
        len: usize,
        data: *mut Zval,
    ) -> *mut Zval;

    fn zend_hash_index_add(ht: *mut ZendHashTable, h: zend_ulong, data: *mut Zval) -> *mut Zval;
}

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

pub fn ht_insert_index(ht: &mut ZendHashTable, index: i64, mut value: Zval) {
    unsafe {
        zend_hash_index_update(ht, index as u64, std::ptr::from_mut(&mut value));
    }

    std::mem::forget(value);
}

/// Inserts only when the key is absent (PHP array-key coercion applies) and
/// reports whether it did; on `false` the value is dropped, not inserted.
pub fn ht_add(ht: &mut ZendHashTable, key: &[u8], mut value: Zval) -> bool {
    let added = unsafe {
        match array_key_index(key) {
            Some(index) => {
                !zend_hash_index_add(ht, index as u64, std::ptr::from_mut(&mut value)).is_null()
            }
            None => !zend_hash_str_add(
                ht,
                key.as_ptr().cast(),
                key.len(),
                std::ptr::from_mut(&mut value),
            )
            .is_null(),
        }
    };

    if added {
        std::mem::forget(value);
    }

    added
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

pub fn zval_null() -> Zval {
    let mut zv = Zval::new();
    zv.set_null();
    zv
}

pub fn ht_contains(ht: &ZendHashTable, key: &[u8]) -> bool {
    match array_key_index(key) {
        Some(index) => ht.get_index(index).is_some(),
        None => unsafe {
            !zend_hash_str_find(
                std::ptr::from_ref(ht).cast_mut(),
                key.as_ptr().cast(),
                key.len(),
            )
            .is_null()
        },
    }
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

/// Reads one property with `BP_VAR_R` semantics; `ZendObject::get_property`
/// passes `BP_VAR_W`, which trips the readonly-property guard on `HydratorColumn`.
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

/// Clone matching PHP `clone` (`zend_objects_clone_members`, including
/// `__clone`). Only valid for plain userland classes (no `create_object` state).
pub fn clone_object(src: &Zval, context: &str) -> Result<ZBox<ZendObject>, PhpException> {
    let src_obj = src
        .object()
        .ok_or_else(|| ext_exception(format!("flow_php expected {context} to be an object")))?;

    let ce = unsafe { src_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception(format!("flow_php failed to resolve {context} class")))?;

    let mut cloned = ZendObject::new(ce);

    unsafe {
        zend_objects_clone_members(
            std::ptr::from_mut(&mut *cloned),
            std::ptr::from_ref(src_obj).cast_mut(),
        );
    }

    Ok(cloned)
}

pub fn construct_object(
    ce: &'static ClassEntry,
    args: Vec<&dyn IntoZvalDyn>,
    context: &str,
) -> Result<ZBox<ZendObject>, PhpException> {
    let obj = ZendObject::new(ce);

    obj.try_call_method("__construct", args)
        .map_err(|e| ext_exception(format!("flow_php failed to construct {context}: {e:?}")))?;
    ensure_no_pending_exception(&format!("construct {context}"))?;

    Ok(obj)
}

fn method_handle(class: &str, method: &str) -> Result<Function, PhpException> {
    Function::try_from_method(class, method).ok_or_else(|| {
        ext_exception(format!(
            "flow_php failed to resolve method {class}::{method}"
        ))
    })
}

fn function_handle(name: &str) -> Result<Function, PhpException> {
    Function::try_from_function(name)
        .ok_or_else(|| ext_exception(format!("flow_php failed to resolve function {name}")))
}

/// Resolves a method to a reference into the class function table WITHOUT
/// copying the zend_function: the engine writes into userland op_arrays
/// (run-time cache), and a copy discards those writes on every call.
fn method_handle_ref(class: &str, method: &str) -> Result<&'static Function, PhpException> {
    ce_method_ref(find_class(class)?, method)
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

pub struct DateTimeFns {
    pub ce: &'static ClassEntry,
    pub set_timezone: Function,
}

pub struct IntervalFns {
    pub ce: &'static ClassEntry,
    pub construct: Function,
    pub spec: Zval,
    pub names: [ZBox<ZendStr>; 8],
}

pub struct Ctx {
    pub entries_ce: &'static ClassEntry,
    pub row_ce: &'static ClassEntry,
    pub rows_ce: &'static ClassEntry,
    pub partitions_ce: &'static ClassEntry,
    pub entries_entries_slot: u32,
    pub row_entries_slot: u32,
    pub rows_rows_slot: u32,
    pub rows_partitions_slot: u32,
    uuid_ce: Option<(&'static ClassEntry, u32)>,
    json_ce: Option<(&'static ClassEntry, u32, u32)>,
    interval: Option<IntervalFns>,
    timezone_ce: Option<&'static ClassEntry>,
    datetime_immutable: Option<DateTimeFns>,
    datetime_mutable: Option<DateTimeFns>,
    datetime_interface_ce: Option<&'static ClassEntry>,
    timezones: HashMap<Vec<u8>, Zval>,
    enums: HashMap<Vec<u8>, Zval>,
    fn_defined: Option<Function>,
    fn_constant: Option<Function>,
    value_decoder_statics: HashMap<&'static str, &'static Function>,
    value_encoder_statics: HashMap<&'static str, &'static Function>,
    schema_decoder: Option<ZBox<ZendObject>>,
    fn_json_encode: Option<Function>,
    encoder_plan_slots: Option<EncoderPlanSlots>,
    encoder_column_slots: Option<EncoderColumnSlots>,
    metadata_map_slot: Option<u32>,
    timezone_get_name: Option<&'static Function>,
    datetime_encode: HashMap<usize, DateTimeEncFns>,
    save_html: HashMap<usize, &'static Function>,
    format_u: Option<Zval>,
    frame_segment: Option<(&'static ClassEntry, &'static Function)>,
}

pub struct DateTimeEncFns {
    pub get_timestamp: &'static Function,
    pub format: &'static Function,
    pub get_timezone: &'static Function,
}

/// Property-table slot offsets on `Flow\Floe\EncoderPlan`.
pub struct EncoderPlanSlots {
    pub schema_body: u32,
    pub columns: u32,
}

/// Property-table slot offsets on `Flow\Floe\EncoderColumn`.
pub struct EncoderColumnSlots {
    pub name: u32,
    pub type_fingerprint: u32,
}

impl Ctx {
    pub fn new() -> Result<Self, PhpException> {
        let entries_ce = find_class("Flow\\ETL\\Row\\Entries")?;
        let row_ce = find_class("Flow\\ETL\\Row")?;
        let rows_ce = find_class("Flow\\ETL\\Rows")?;

        Ok(Self {
            entries_ce,
            row_ce,
            rows_ce,
            partitions_ce: find_class("Flow\\Filesystem\\Partitions")?,
            entries_entries_slot: property_offset(entries_ce, "entries")?,
            row_entries_slot: property_offset(row_ce, "entries")?,
            rows_rows_slot: property_offset(rows_ce, "rows")?,
            rows_partitions_slot: property_offset(rows_ce, "partitions")?,
            uuid_ce: None,
            json_ce: None,
            interval: None,
            timezone_ce: None,
            datetime_immutable: None,
            datetime_mutable: None,
            datetime_interface_ce: None,
            timezones: HashMap::new(),
            enums: HashMap::new(),
            fn_defined: None,
            fn_constant: None,
            value_decoder_statics: HashMap::new(),
            value_encoder_statics: HashMap::new(),
            schema_decoder: None,
            fn_json_encode: None,
            encoder_plan_slots: None,
            encoder_column_slots: None,
            metadata_map_slot: None,
            timezone_get_name: None,
            datetime_encode: HashMap::new(),
            save_html: HashMap::new(),
            format_u: None,
            frame_segment: None,
        })
    }

    /// Cached `Flow\Floe\FrameSegment` class entry + constructor handle.
    pub fn frame_segment(
        &mut self,
    ) -> Result<(&'static ClassEntry, &'static Function), PhpException> {
        if self.frame_segment.is_none() {
            let ce = find_class("Flow\\Floe\\FrameSegment")?;
            self.frame_segment = Some((ce, ce_method_ref(ce, "__construct")?));
        }

        Ok(self.frame_segment.expect("just initialized"))
    }

    pub fn metadata_map_slot(&mut self) -> Result<u32, PhpException> {
        if self.metadata_map_slot.is_none() {
            self.metadata_map_slot = Some(property_offset(
                find_class("Flow\\ETL\\Schema\\Metadata")?,
                "map",
            )?);
        }

        Ok(self.metadata_map_slot.expect("just initialized"))
    }

    pub fn timezone_get_name(&mut self) -> Result<&'static Function, PhpException> {
        if self.timezone_get_name.is_none() {
            self.timezone_get_name = Some(method_handle_ref("DateTimeZone", "getName")?);
        }

        Ok(self.timezone_get_name.expect("just initialized"))
    }

    pub fn datetime_interface(&mut self) -> Result<&'static ClassEntry, PhpException> {
        if self.datetime_interface_ce.is_none() {
            self.datetime_interface_ce = Some(find_class("DateTimeInterface")?);
        }

        Ok(self.datetime_interface_ce.expect("just initialized"))
    }

    /// Per-datetime-class encode handles (getTimestamp/format/getTimezone).
    pub fn datetime_encode_fns(
        &mut self,
        ce: *const ClassEntry,
    ) -> Result<&DateTimeEncFns, PhpException> {
        let key = ce as usize;

        if let std::collections::hash_map::Entry::Vacant(entry) = self.datetime_encode.entry(key) {
            let ce_ref = unsafe { ce.as_ref() }
                .ok_or_else(|| ext_exception("flow_php failed to resolve a datetime class"))?;

            entry.insert(DateTimeEncFns {
                get_timestamp: ce_method_ref(ce_ref, "getTimestamp")?,
                format: ce_method_ref(ce_ref, "format")?,
                get_timezone: ce_method_ref(ce_ref, "getTimezone")?,
            });
        }

        Ok(self.datetime_encode.get(&key).expect("just inserted"))
    }

    pub fn save_html(&mut self, ce: *const ClassEntry) -> Result<&'static Function, PhpException> {
        let key = ce as usize;

        if let std::collections::hash_map::Entry::Vacant(entry) = self.save_html.entry(key) {
            let ce_ref = unsafe { ce.as_ref() }
                .ok_or_else(|| ext_exception("flow_php failed to resolve an HTML class"))?;
            entry.insert(ce_method_ref(ce_ref, "saveHtml")?);
        }

        Ok(self.save_html.get(&key).expect("just inserted"))
    }

    /// Cached `"u"` format-string zval for microsecond reads.
    pub fn format_u(&mut self) -> Result<&Zval, PhpException> {
        if self.format_u.is_none() {
            self.format_u = Some(zval_str(b"u"));
        }

        Ok(self.format_u.as_ref().expect("just initialized"))
    }

    /// Cached handles to `ValueEncoder`'s public static markup serializers.
    pub fn value_encoder_static(
        &mut self,
        method: &'static str,
    ) -> Result<&'static Function, PhpException> {
        if !self.value_encoder_statics.contains_key(method) {
            let handle = method_handle_ref("Flow\\Floe\\ValueEncoder", method)?;
            self.value_encoder_statics.insert(method, handle);
        }

        Ok(self
            .value_encoder_statics
            .get(method)
            .expect("just inserted"))
    }

    pub fn json(&mut self) -> Result<(&'static ClassEntry, u32, u32), PhpException> {
        if self.json_ce.is_none() {
            let ce = find_class("Flow\\Types\\Value\\Json")?;
            self.json_ce = Some((
                ce,
                property_offset(ce, "value")?,
                property_offset(ce, "isObject")?,
            ));
        }

        Ok(self.json_ce.expect("just initialized"))
    }

    /// Enum cases cached per `Class::Case`, resolved with the same validation
    /// and errors as `ValueDecoder` (enum_exists + defined + constant).
    pub fn enum_case(&mut self, class: &[u8], case: &[u8]) -> Result<Zval, PhpException> {
        let mut cache_key = Vec::with_capacity(class.len() + case.len() + 2);
        cache_key.extend_from_slice(class);
        cache_key.extend_from_slice(b"::");
        cache_key.extend_from_slice(case);

        if let Some(cached) = self.enums.get(&cache_key) {
            return Ok(cached.shallow_clone());
        }

        let constant_name = std::str::from_utf8(&cache_key)
            .map_err(|_| ext_exception("flow_php expected an enum name to be UTF-8"))?
            .to_string();
        let class_name = std::str::from_utf8(class)
            .expect("validated above")
            .to_string();
        let case_name = std::str::from_utf8(case)
            .expect("validated above")
            .to_string();

        let is_enum = ClassEntry::try_find(&class_name)
            .is_some_and(|ce| ce.flags().contains(ClassFlags::Enum));

        if !is_enum {
            return Err(ext_exception(format!(
                "flow_php cannot restore enum of class \"{class_name}\", enum not found"
            )));
        }

        if self.fn_defined.is_none() {
            self.fn_defined = Some(function_handle("defined")?);
            self.fn_constant = Some(function_handle("constant")?);
        }

        let constant_name_zv = zval_str(constant_name.as_bytes());

        let defined = call_handle(
            &self.fn_defined.expect("just initialized"),
            None,
            &mut [constant_name_zv.shallow_clone()],
            "check an enum case",
        )?;

        if !defined.bool().unwrap_or(false) {
            return Err(ext_exception(format!(
                "flow_php cannot restore enum case \"{class_name}::{case_name}\""
            )));
        }

        let case_zv = call_handle(
            &self.fn_constant.expect("just initialized"),
            None,
            &mut [constant_name_zv],
            "restore an enum case",
        )?;

        if !case_zv.is_object() {
            return Err(ext_exception(format!(
                "flow_php cannot restore enum case \"{class_name}::{case_name}\""
            )));
        }

        let result = case_zv.shallow_clone();
        self.enums.insert(cache_key, case_zv);

        Ok(result)
    }

    /// Cached handles to `ValueDecoder`'s public static markup restorers.
    pub fn value_decoder_static(
        &mut self,
        method: &'static str,
    ) -> Result<&'static Function, PhpException> {
        if !self.value_decoder_statics.contains_key(method) {
            let handle = method_handle_ref("Flow\\Floe\\ValueDecoder", method)?;
            self.value_decoder_statics.insert(method, handle);
        }

        Ok(self
            .value_decoder_statics
            .get(method)
            .expect("just inserted"))
    }

    pub fn uuid(&mut self) -> Result<(&'static ClassEntry, u32), PhpException> {
        if self.uuid_ce.is_none() {
            let ce = find_class("Flow\\Types\\Value\\Uuid")?;
            self.uuid_ce = Some((ce, property_offset(ce, "value")?));
        }

        Ok(self.uuid_ce.expect("just initialized"))
    }

    pub fn interval(&mut self) -> Result<&mut IntervalFns, PhpException> {
        if self.interval.is_none() {
            self.interval = Some(IntervalFns {
                ce: find_class("DateInterval")?,
                construct: method_handle("DateInterval", "__construct")?,
                spec: zval_str(b"PT0S"),
                names: [
                    ZendStr::new("y", false),
                    ZendStr::new("m", false),
                    ZendStr::new("d", false),
                    ZendStr::new("h", false),
                    ZendStr::new("i", false),
                    ZendStr::new("s", false),
                    ZendStr::new("invert", false),
                    ZendStr::new("f", false),
                ],
            });
        }

        Ok(self.interval.as_mut().expect("just initialized"))
    }

    /// Datetime class entry + pre-resolved setTimezone handle per class.
    pub fn datetime_fns(&mut self, mutable: bool) -> Result<&DateTimeFns, PhpException> {
        let slot = if mutable {
            &mut self.datetime_mutable
        } else {
            &mut self.datetime_immutable
        };

        if slot.is_none() {
            let class = if mutable {
                "DateTime"
            } else {
                "DateTimeImmutable"
            };
            *slot = Some(DateTimeFns {
                ce: find_class(class)?,
                set_timezone: method_handle(class, "setTimezone")?,
            });
        }

        Ok(slot.as_ref().expect("just initialized"))
    }

    /// `DateTimeZone` instances cached per timezone name, mirroring
    /// `ValueDecoder::$timezones`.
    pub fn timezone(&mut self, name: &[u8]) -> Result<&Zval, PhpException> {
        if !self.timezones.contains_key(name) {
            if self.timezone_ce.is_none() {
                self.timezone_ce = Some(find_class("DateTimeZone")?);
            }

            let name_string = std::str::from_utf8(name)
                .map_err(|_| ext_exception("flow_php found a non UTF-8 timezone name"))?
                .to_string();
            let mut timezone = construct_object(
                self.timezone_ce.expect("just initialized"),
                vec![&name_string as &dyn IntoZvalDyn],
                "DateTimeZone",
            )?;

            let mut zv = Zval::new();
            zv.set_object(&mut timezone);
            self.timezones.insert(name.to_vec(), zv);
        }

        Ok(self.timezones.get(name).expect("just inserted"))
    }

    /// The canonical PHP schema decoder, constructed lazily - schema frames are
    /// the cold path and reusing `SchemaDecoder` prevents semantic drift.
    pub fn schema_decoder(&mut self) -> Result<&ZBox<ZendObject>, PhpException> {
        if self.schema_decoder.is_none() {
            let mut value_decoder = construct_object(
                find_class("Flow\\Floe\\ValueDecoder")?,
                vec![],
                "Flow\\Floe\\ValueDecoder",
            )?;
            // EntryInstantiator declares no constructor; defaults apply on init.
            let mut entry_instantiator =
                ZendObject::new(find_class("Flow\\Floe\\EntryInstantiator")?);

            let mut value_decoder_zv = Zval::new();
            value_decoder_zv.set_object(&mut value_decoder);
            let mut entry_instantiator_zv = Zval::new();
            entry_instantiator_zv.set_object(&mut entry_instantiator);

            self.schema_decoder = Some(construct_object(
                find_class("Flow\\Floe\\SchemaDecoder")?,
                vec![
                    &value_decoder_zv as &dyn IntoZvalDyn,
                    &entry_instantiator_zv as &dyn IntoZvalDyn,
                ],
                "Flow\\Floe\\SchemaDecoder",
            )?);
        }

        Ok(self.schema_decoder.as_ref().expect("just initialized"))
    }

    pub fn json_encode(&mut self) -> Result<&Function, PhpException> {
        if self.fn_json_encode.is_none() {
            self.fn_json_encode = Some(function_handle("json_encode")?);
        }

        Ok(self.fn_json_encode.as_ref().expect("just initialized"))
    }

    pub fn encoder_plan_slots(&mut self) -> Result<&EncoderPlanSlots, PhpException> {
        if self.encoder_plan_slots.is_none() {
            let ce = find_class("Flow\\Floe\\EncoderPlan")?;
            self.encoder_plan_slots = Some(EncoderPlanSlots {
                schema_body: property_offset(ce, "schemaBody")?,
                columns: property_offset(ce, "columns")?,
            });
        }

        Ok(self.encoder_plan_slots.as_ref().expect("just initialized"))
    }

    pub fn encoder_column_slots(&mut self) -> Result<&EncoderColumnSlots, PhpException> {
        if self.encoder_column_slots.is_none() {
            let ce = find_class("Flow\\Floe\\EncoderColumn")?;
            self.encoder_column_slots = Some(EncoderColumnSlots {
                name: property_offset(ce, "name")?,
                type_fingerprint: property_offset(ce, "typeFingerprint")?,
            });
        }

        Ok(self
            .encoder_column_slots
            .as_ref()
            .expect("just initialized"))
    }
}
