use std::rc::Rc;
use std::sync::Arc;

use ahash::AHashMap;
use arrow_array::{Int64Array, NullArray};
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::canonical_rows::{named_column, ordered, RowColumn};
use crate::column::RustColumn;
use crate::ctx::{call_method, construct, find_class, instance, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::interfaces::group_table_ce;
use crate::plan::{type_plan, TypePlan};

const GROUP_IDS: &str = "Flow\\ETL\\Values\\Grouping\\GroupIds";
const INTEGER_TYPE: &str = "Flow\\Types\\Type\\Native\\IntegerType";
const NULL_TYPE: &str = "Flow\\Types\\Type\\Native\\NullType";
const VALUE_EQUALITY: &str = "Flow\\ETL\\Values\\Column\\ValueEquality";

pub struct GroupIdsValue(Zval);

impl IntoZval for GroupIdsValue {
    const TYPE: DataType = DataType::object(GROUP_IDS);
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Grouping\\RustGroupTable",
    flags = ClassFlags::Final,
    implements(ce = group_table_ce, stub = "Flow\\ETL\\Values\\Grouping\\GroupTable")
)]
pub struct RustGroupTable {
    columns: Vec<Zval>,
    /// The declared type of each key column, by position, that its batches are cast to as `KeyEncoder::columns()` does.
    declared_types: Vec<Option<(Zval, Rc<TypePlan>)>>,
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
        let declared_types = columns
            .iter()
            .map(|name| {
                let definition = call_method(declared, "findDefinition", &mut [name.shallow_clone()])?;

                if definition.is_null() {
                    return Ok(None);
                }

                let declared_type = call_method(&definition, "type", &mut [])?;
                let plan = type_plan(&declared_type)?;

                Ok(Some((declared_type, plan)))
            })
            .collect::<PhpResult<Vec<_>>>()?;

        Ok(Self {
            columns,
            declared_types,
            declared: declared.shallow_clone(),
            ids: AHashMap::new(),
        })
    }

    fn key_column(&self, rows: &Zval, schema: &Zval, position: usize, count: usize) -> PhpResult<RowColumn> {
        let name = &self.columns[position];

        if call_method(schema, "findDefinition", &mut [name.shallow_clone()])?.is_null()
            && self.declared_nullable(name)?
        {
            let nulls = RustColumn::of_type(Arc::new(NullArray::new(count)), NULL_TYPE)?.into_zval(false)?;

            return named_column(Vec::new(), &nulls);
        }

        let column = call_method(rows, "column", &mut [name.shallow_clone()])?;
        let key = named_column(Vec::new(), &column)?;

        match &self.declared_types[position] {
            Some((declared_type, plan)) if plan.type_name != key.2.type_name => named_column(
                Vec::new(),
                &call_method(
                    &instance(VALUE_EQUALITY)?,
                    "cast",
                    &mut [column, declared_type.shallow_clone(), name.shallow_clone()],
                )?,
            ),
            _ => Ok(key),
        }
    }

    fn declared_nullable(&self, name: &Zval) -> PhpResult<bool> {
        let definition = call_method(&self.declared, "findDefinition", &mut [name.shallow_clone()])?;

        Ok(!definition.is_null() && call_method(&definition, "isNullable", &mut [])?.bool() == Some(true))
    }

    fn each_key(
        &self,
        rows: &Zval,
        count: usize,
        mut visit: impl FnMut(usize, &[u8]) -> PhpResult<()>,
    ) -> PhpResult<()> {
        if count == 0 {
            return Ok(());
        }

        let schema = call_method(rows, "schema", &mut [])?;
        let columns = (0..self.columns.len())
            .map(|position| self.key_column(rows, &schema, position, count))
            .collect::<PhpResult<Vec<_>>>()?;
        let mut key = Vec::new();
        let mut entries = Vec::new();

        for row in 0..count {
            ordered(&mut key, &mut entries, &columns, row);
            visit(row, &key)?;
        }

        Ok(())
    }
}

fn row_count(rows: &Zval) -> PhpResult<usize> {
    call_method(rows, "count", &mut [])?
        .long()
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))
}

#[php_impl]
impl RustGroupTable {
    pub fn assign(&mut self, rows: &Zval) -> PhpResult<GroupIdsValue> {
        let count = row_count(rows)?;
        let mut ids = Vec::with_capacity(count);
        let mut first_rows = ZendHashTable::new();
        let mut table = std::mem::take(&mut self.ids);
        let visited = self.each_key(rows, count, |row, key| {
            ids.push(match table.get(key) {
                Some(&id) => id,
                None => {
                    let id = table.len() as i64;
                    table.insert(key.into(), id);
                    first_rows.push(zval_long(row as i64))?;

                    id
                }
            });

            Ok(())
        });
        self.ids = table;
        visited?;

        let ids = RustColumn::of_type(Arc::new(Int64Array::from(ids)), INTEGER_TYPE)?.into_zval(false)?;
        let mut first_rows_zv = Zval::new();
        first_rows_zv.set_hashtable(first_rows);

        Ok(GroupIdsValue(
            construct(find_class(GROUP_IDS)?, &mut [ids, first_rows_zv])?.into_zval(false)?,
        ))
    }

    pub fn lookup(&self, rows: &Zval) -> PhpResult<RustColumn> {
        let count = row_count(rows)?;
        let mut ids = Vec::with_capacity(count);

        self.each_key(rows, count, |_, key| {
            ids.push(self.ids.get(key).copied());

            Ok(())
        })?;

        RustColumn::of_type(Arc::new(Int64Array::from(ids)), INTEGER_TYPE)
    }

    pub fn count(&self) -> i64 {
        self.ids.len() as i64
    }
}
