use std::sync::Arc;

use ahash::AHashMap;
use arrow_array::{ArrayRef, Int64Array, NullArray};
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_column_kernels::grouping::{append_encoded, append_native_key};

use crate::backend::RustBackend;
use crate::column::RustColumn;
use crate::ctx::{call_method, construct, expect_object, find_class, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::interfaces::group_table_ce;
use crate::plan::type_plan;

const GROUP_IDS: &str = "Flow\\ETL\\Values\\Grouping\\GroupIds";
const INTEGER_TYPE: &str = "Flow\\Types\\Type\\Native\\IntegerType";
const KEY_ENCODER: &str = "Flow\\ETL\\Values\\Grouping\\KeyEncoder";
const MIXED_TYPE: &str = "Flow\\Types\\Type\\Native\\MixedType";

// types whose stored physical is what ComparableValues::equality() compares; xml and html compare by TextValues
const NATIVE_KEYS: [&str; 15] = [
    "integer",
    "positive_integer",
    "float",
    "boolean",
    "string",
    "non_empty_string",
    "numeric-string",
    "class_string",
    "json",
    "enum",
    "uuid",
    "datetime",
    "date",
    "time",
    "null",
];

pub struct GroupIdsValue(Zval);

impl IntoZval for GroupIdsValue {
    const TYPE: DataType = DataType::object(GROUP_IDS);
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

enum KeyColumn {
    Native(ArrayRef),
    Encoded(Vec<Vec<u8>>),
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Grouping\\RustGroupTable",
    flags = ClassFlags::Final,
    implements(ce = group_table_ce, stub = "Flow\\ETL\\Values\\Grouping\\GroupTable")
)]
pub struct RustGroupTable {
    columns: Vec<Zval>,
    declared: Zval,
    ids: AHashMap<Box<[u8]>, i64>,
}

impl RustGroupTable {
    pub fn new(columns: &ZendHashTable, declared: &Zval) -> PhpResult<Self> {
        let columns = columns
            .values()
            .map(|name| {
                name.zend_str()
                    .map(|name| zval_str(name.as_bytes()))
                    .ok_or_else(|| ext_exception("flow_php expected group by columns as strings"))
            })
            .collect::<PhpResult<Vec<_>>>()?;

        Ok(Self {
            columns,
            declared: declared.shallow_clone(),
            ids: AHashMap::new(),
        })
    }

    fn key_column(&self, rows: &Zval, schema: &Zval, name: &Zval, count: usize) -> PhpResult<KeyColumn> {
        if call_method(schema, "findDefinition", &mut [name.shallow_clone()])?.is_null()
            && self.declared_nullable(name)?
        {
            return Ok(KeyColumn::Native(Arc::new(NullArray::new(count))));
        }

        let column = call_method(rows, "column", &mut [name.shallow_clone()])?;

        if let Some(native) = column.extract::<&RustColumn>() {
            return if NATIVE_KEYS.contains(&native.plan().type_name.as_str()) {
                Ok(KeyColumn::Native(Arc::clone(native.data())))
            } else {
                self.encoded(rows, name)
            };
        }

        let column_type = call_method(&column, "type", &mut [])?;

        if expect_object(&column_type, "a Type")?.instance_of(find_class(MIXED_TYPE)?)
            || !NATIVE_KEYS.contains(&type_plan(&column_type)?.type_name.as_str())
        {
            return self.encoded(rows, name);
        }

        // adopted, so a column keeps one encoding whichever backend built its batch
        let definition = call_method(schema, "get", &mut [name.shallow_clone()])?;
        let adopted = RustBackend.adopt(&definition, &column)?.0;

        Ok(KeyColumn::Native(Arc::clone(
            adopted
                .extract::<&RustColumn>()
                .ok_or_else(|| ext_exception("flow_php expected RustBackend::adopt() to return a RustColumn"))?
                .data(),
        )))
    }

    fn declared_nullable(&self, name: &Zval) -> PhpResult<bool> {
        let definition = call_method(&self.declared, "findDefinition", &mut [name.shallow_clone()])?;

        Ok(!definition.is_null() && call_method(&definition, "isNullable", &mut [])?.bool() == Some(true))
    }

    fn encoded(&self, rows: &Zval, name: &Zval) -> PhpResult<KeyColumn> {
        let mut names = ZendHashTable::new();
        names.push(name.shallow_clone())?;
        let mut names_zv = Zval::new();
        names_zv.set_hashtable(names);
        let mut types = Zval::new();
        types.set_hashtable(ZendHashTable::new());
        let encoder = construct(
            find_class(KEY_ENCODER)?,
            &mut [names_zv, types, self.declared.shallow_clone()],
        )?
        .into_zval(false)?;
        let keys = call_method(&encoder, "of", &mut [rows.shallow_clone()])?;

        Ok(KeyColumn::Encoded(
            keys.array()
                .ok_or_else(|| ext_exception("flow_php expected KeyEncoder::of() to return an array"))?
                .values()
                .map(|key| {
                    key.zend_str()
                        .map(|key| key.as_bytes().to_vec())
                        .ok_or_else(|| ext_exception("flow_php expected KeyEncoder::of() to return strings"))
                })
                .collect::<PhpResult<Vec<_>>>()?,
        ))
    }
}

#[php_impl]
impl RustGroupTable {
    pub fn assign(&mut self, rows: &Zval) -> PhpResult<GroupIdsValue> {
        let count = call_method(rows, "count", &mut [])?
            .long()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))?;
        let mut ids = Vec::with_capacity(count);
        let mut first_rows = ZendHashTable::new();

        if count > 0 {
            let schema = call_method(rows, "schema", &mut [])?;
            let columns = self
                .columns
                .iter()
                .map(|name| self.key_column(rows, &schema, name, count))
                .collect::<PhpResult<Vec<_>>>()?;
            let mut key = Vec::new();

            for row in 0..count {
                key.clear();

                for column in &columns {
                    match column {
                        KeyColumn::Native(array) => {
                            if !append_native_key(&mut key, array.as_ref(), row) {
                                return Err(ext_exception(format!(
                                    "flow_php has no group key for {} values",
                                    array.data_type()
                                )));
                            }
                        }
                        KeyColumn::Encoded(keys) => append_encoded(&mut key, &keys[row]),
                    }
                }

                ids.push(match self.ids.get(key.as_slice()) {
                    Some(&id) => id,
                    None => {
                        let id = self.ids.len() as i64;
                        self.ids.insert(key.as_slice().into(), id);
                        first_rows.push(zval_long(row as i64))?;

                        id
                    }
                });
            }
        }

        let ids = RustColumn::of_type(Arc::new(Int64Array::from(ids)), INTEGER_TYPE)?.into_zval(false)?;
        let mut first_rows_zv = Zval::new();
        first_rows_zv.set_hashtable(first_rows);

        Ok(GroupIdsValue(
            construct(find_class(GROUP_IDS)?, &mut [ids, first_rows_zv])?.into_zval(false)?,
        ))
    }

    pub fn count(&self) -> i64 {
        self.ids.len() as i64
    }
}
