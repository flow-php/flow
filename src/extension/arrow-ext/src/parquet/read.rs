//! Row groups of a Parquet file as canonical arrays: only the groups an offset/limit window overlaps are decoded, and
//! every projected column is checked against the cast table before the first batch.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{make_array, Array, ArrayRef, RecordBatchReader};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, Type};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::schema::types::SchemaDescriptor;

use crate::parquet::canonical::{canonical, canonical_type};
use crate::parquet::error::Error;
use crate::parquet::fetch::ChunkFetch;
use crate::parquet::source::ReadAt;

/// The row groups overlapping [offset, offset + limit) and the rows to skip in the first of them.
#[derive(Debug, PartialEq, Eq)]
pub struct ReadPlan {
    pub row_groups: Vec<usize>,
    pub skip: usize,
    pub limit: Option<usize>,
}

impl ReadPlan {
    pub fn new(meta: &ParquetMetaData, offset: u64, limit: Option<u64>) -> Self {
        let (mut row_groups, mut skip, mut start) = (Vec::new(), 0, 0);

        for (index, group) in meta.row_groups().iter().enumerate() {
            let end = start + group.num_rows() as u64;

            if end > offset && limit.is_none_or(|limit| start < offset + limit) {
                if row_groups.is_empty() {
                    skip = (offset - start) as usize;
                }

                row_groups.push(index);
            }

            start = end;
        }

        Self {
            row_groups,
            skip,
            limit: limit.map(|limit| limit as usize),
        }
    }
}

/// A projected column: a root name, or a path through structs ("structure.int64") - the root first, then the child
/// names down to the column.
struct Projected {
    path: Vec<String>,
    field: FieldRef,
}

impl Projected {
    /// A root named `name` as it is; else `name` as a path whose every segment but the last is a struct.
    fn resolve(schema: &Schema, name: &str) -> Result<Self, Error> {
        if let Ok(root) = schema.field_with_name(name) {
            return Ok(Self {
                path: vec![name.to_string()],
                field: Arc::new(root.clone()),
            });
        }

        let missing = || Error::MissingColumn(name.to_string());
        let mut segments = name.split('.');
        let root = segments.next().ok_or_else(missing)?;
        let mut field = schema.field_with_name(root).map_err(|_| missing())?;
        let mut nullable = field.is_nullable();
        let mut path = vec![root.to_string()];

        for segment in segments {
            field = match field.data_type() {
                DataType::Struct(children) => children.find(segment).ok_or_else(missing)?.1,
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) | DataType::Map(_, _) => {
                    return Err(Error::Unsupported {
                        column: name.to_string(),
                        parquet: "a path into LIST/MAP".to_string(),
                    })
                }
                _ => return Err(missing()),
            };
            nullable |= field.is_nullable();
            path.push(segment.to_string());
        }

        Ok(Self {
            path,
            field: Arc::new(field.clone().with_name(name).with_nullable(nullable)),
        })
    }

    fn reads_leaf(&self, parts: &[String]) -> bool {
        parts.starts_with(&self.path)
    }
}

pub struct ParquetReader {
    batches: ParquetRecordBatchReader,
    /// Each column's field as the file stores it, named as it was asked for.
    fields: Vec<FieldRef>,
    /// `canonical_type()` of each of `fields`.
    types: Vec<DataType>,
    /// Where each of `fields` sits in the projected batches: its root, then the struct children down to it.
    positions: Vec<(usize, Vec<String>)>,
}

impl ParquetReader {
    /// `source`: the file's bytes (`PhpStream`), `size` long; `meta`: its footer, which `plan` was built from. Only
    /// the projected column chunks of the planned row groups are fetched (`ChunkFetch`); the page index is read on a
    /// copy of `meta`, only when the plan skips rows inside its first row group.
    pub fn open<R: ReadAt + Send + Sync + 'static>(
        source: Arc<R>,
        size: u64,
        meta: &Arc<ParquetMetaData>,
        columns: &[String],
        plan: ReadPlan,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let inferred = ArrowReaderMetadata::try_new(Arc::clone(meta), ArrowReaderOptions::new())?;
        let options = match int96_hint(inferred.schema(), meta.file_metadata().schema_descr()) {
            Some(hint) => ArrowReaderOptions::new().with_schema(hint),
            None => ArrowReaderOptions::new(),
        };
        let schema = ArrowReaderMetadata::try_new(Arc::clone(meta), options.clone())?.schema().clone();
        let projected = columns
            .iter()
            .map(|name| Projected::resolve(&schema, name))
            .collect::<Result<Vec<_>, _>>()?;
        let fields = projected.iter().map(|column| Arc::clone(&column.field)).collect::<Vec<_>>();
        let types = fields.iter().map(|field| canonical_type(field)).collect::<Result<Vec<_>, _>>()?;

        if let Some(codec) = unsupported_codec(meta, &plan.row_groups, &projected) {
            return Err(codec);
        }

        let descr = meta.file_metadata().schema_descr();
        let leaves = (0..descr.num_columns())
            .filter(|leaf| projected.iter().any(|column| column.reads_leaf(descr.column(*leaf).path().parts())))
            .collect::<Vec<_>>();
        let fetch = ChunkFetch::new(source, size, meta, &plan.row_groups, &leaves);
        let meta = if plan.skip > 0 {
            let mut reader = ParquetMetaDataReader::new_with_metadata(meta.as_ref().clone())
                .with_page_index_policy(PageIndexPolicy::Optional);
            reader.read_page_indexes(&fetch)?;
            ArrowReaderMetadata::try_new(Arc::new(reader.finish()?), options)?
        } else {
            ArrowReaderMetadata::try_new(Arc::clone(meta), options)?
        };
        let mask = ProjectionMask::leaves(meta.metadata().file_metadata().schema_descr(), leaves);
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(fetch, meta)
            .with_row_groups(plan.row_groups)
            .with_projection(mask)
            .with_batch_size(batch_size);

        if plan.skip > 0 {
            builder = builder.with_offset(plan.skip);
        }

        if let Some(limit) = plan.limit {
            builder = builder.with_limit(limit);
        }

        let batches = builder.build()?;
        let schema = batches.schema();
        let positions = projected
            .into_iter()
            .map(|mut column| {
                let root = schema.index_of(&column.path[0]).map_err(|_| Error::MissingColumn(column.field.name().clone()))?;
                column.path.remove(0);

                Ok((root, column.path))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(Self {
            batches,
            fields,
            types,
            positions,
        })
    }

    /// The canonical type each of `fields()` is read as.
    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    pub fn fields(&self) -> &[FieldRef] {
        &self.fields
    }

    pub fn next(&mut self) -> Option<Result<(usize, Vec<ArrayRef>), Error>> {
        let batch = match self.batches.next()? {
            Ok(batch) => batch,
            Err(error) => return Some(Err(error.into())),
        };

        Some(
            self.fields
                .iter()
                .zip(&self.positions)
                .map(|(field, (root, children))| canonical(field, &child(batch.column(*root), children)?))
                .collect::<Result<Vec<_>, _>>()
                .map(|columns| (batch.num_rows(), columns)),
        )
    }
}

/// The struct child at `path` under `array`, null wherever a struct above it is.
fn child(array: &ArrayRef, path: &[String]) -> Result<ArrayRef, Error> {
    let mut array = Arc::clone(array);

    for name in path {
        let structure = array.as_struct();
        let child = structure
            .column_by_name(name)
            .ok_or_else(|| Error::MissingColumn(name.clone()))?;

        array = match NullBuffer::union(structure.nulls(), child.nulls()) {
            Some(nulls) if structure.nulls().is_some() => {
                make_array(child.to_data().into_builder().nulls(Some(nulls)).build()?)
            }
            _ => Arc::clone(child),
        };
    }

    Ok(array)
}

/// The inferred schema with every INT96 leaf read as `Timestamp(µs, "UTC")` - exact over years 1 to 9999, where
/// arrow-rs's default nanoseconds wrap outside 1677-2262; `None` without INT96 leaves.
fn int96_hint(schema: &SchemaRef, descr: &SchemaDescriptor) -> Option<SchemaRef> {
    let mut leaf = 0;
    let mut hinted = false;
    let fields = schema
        .fields()
        .iter()
        .map(|field| int96_hinted(field, descr, &mut leaf, &mut hinted))
        .collect::<Fields>();

    hinted.then(|| Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
}

/// `field` with its INT96 leaves hinted; `leaf` counts the Parquet leaves `field`'s arrow leaves stand for, in order.
fn int96_hinted(field: &FieldRef, descr: &SchemaDescriptor, leaf: &mut usize, hinted: &mut bool) -> FieldRef {
    let mut hint = |field: &FieldRef| int96_hinted(field, descr, leaf, hinted);
    let data_type = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(children.iter().map(&mut hint).collect()),
        DataType::List(element) => DataType::List(hint(element)),
        DataType::LargeList(element) => DataType::LargeList(hint(element)),
        DataType::FixedSizeList(element, size) => DataType::FixedSizeList(hint(element), *size),
        DataType::Map(entries, sorted) => DataType::Map(hint(entries), *sorted),
        _ => {
            let index = *leaf;
            *leaf += 1;

            if index >= descr.num_columns() || descr.column(index).physical_type() != Type::INT96 {
                return Arc::clone(field);
            }

            *hinted = true;
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
    };

    Arc::new(Field::clone(field).with_data_type(data_type))
}

/// A codec the parquet crate is built without, in a projected column chunk of the planned row groups.
fn unsupported_codec(meta: &ParquetMetaData, row_groups: &[usize], columns: &[Projected]) -> Option<Error> {
    row_groups.iter().find_map(|group| {
        meta.row_group(*group).columns().iter().find_map(|chunk| {
            let column = columns.iter().find(|column| column.reads_leaf(chunk.column_path().parts()))?;

            (chunk.compression() == Compression::LZO).then(|| Error::Unsupported {
                column: column.field.name().clone(),
                parquet: "LZO".to_string(),
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::TimestampMicrosecondType;
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::data_type::{Int64Type, Int96, Int96Type};
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{int96_hint, unsupported_codec, ParquetReader, Projected, ReadPlan};
    use crate::parquet::error::Error;
    use crate::parquet::footer::Footer;
    use crate::parquet::source::fake::Counting;

    fn eight_groups_of_8192() -> parquet::file::metadata::ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut file,
            Arc::clone(&schema),
            Some(WriterProperties::builder().set_max_row_group_row_count(Some(8_192)).build()),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..65_536))]).unwrap())
            .unwrap();
        writer.close().unwrap();

        ParquetMetaDataReader::new().parse_and_finish(&Bytes::from(file)).unwrap()
    }

    fn plan(offset: u64, limit: u64) -> (Vec<usize>, usize) {
        let plan = ReadPlan::new(&eight_groups_of_8192(), offset, Some(limit));

        assert_eq!(plan.limit, Some(limit as usize));

        (plan.row_groups, plan.skip)
    }

    #[test]
    fn plan_reads_the_groups_a_window_overlaps() {
        assert_eq!(eight_groups_of_8192().row_groups().len(), 8);
        assert_eq!(plan(0, 10), (vec![0], 0));
        assert_eq!(plan(5_399, 3), (vec![0], 5_399));
        assert_eq!(plan(20_000, 15_000), (vec![2, 3, 4], 3_616));
        assert_eq!(plan(65_000, 100), (vec![7], 7_656));
        assert_eq!(plan(65_046, 1), (vec![7], 7_702));
    }

    #[test]
    fn plan_without_limit_reads_every_group_from_the_offset() {
        let plan = ReadPlan::new(&eight_groups_of_8192(), 16_384, None);

        assert_eq!(
            plan,
            ReadPlan {
                row_groups: vec![2, 3, 4, 5, 6, 7],
                skip: 0,
                limit: None
            }
        );
    }

    fn with_lzo_names() -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, Arc::clone(&schema), None).unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![Arc::new(Int64Array::from(vec![1])), Arc::new(StringArray::from(vec!["a"]))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        let mut builder = ParquetMetaDataReader::new().parse_and_finish(&Bytes::from(file)).unwrap().into_builder();
        let row_groups = builder
            .take_row_groups()
            .into_iter()
            .map(|group| {
                let columns = group
                    .columns()
                    .iter()
                    .map(|chunk| match chunk.column_path().string().as_str() {
                        "name" => chunk.clone().into_builder().set_compression(Compression::LZO).build().unwrap(),
                        _ => chunk.clone(),
                    })
                    .collect();

                group.into_builder().set_column_metadata(columns).build().unwrap()
            })
            .collect();

        builder.set_row_groups(row_groups).build()
    }

    #[test]
    fn an_lzo_column_chunk_is_refused_when_projected() {
        let meta = with_lzo_names();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let column = |name: &str| Projected::resolve(&schema, name).unwrap();

        assert!(matches!(
            unsupported_codec(&meta, &[0], &[column("id"), column("name")]),
            Some(Error::Unsupported { column, parquet }) if column == "name" && parquet == "LZO"
        ));
        assert!(unsupported_codec(&meta, &[0], &[column("id")]).is_none());
    }

    fn open(file: Vec<u8>, columns: &[&str], offset: u64, limit: Option<u64>) -> Result<(ParquetReader, Arc<Counting>), Error> {
        let footer = Footer::read(&Bytes::from(file.clone()))?;
        let source = Arc::new(Counting::new(file));
        let size = source.data.len() as u64;
        let columns = columns.iter().map(|column| column.to_string()).collect::<Vec<_>>();
        let plan = ReadPlan::new(footer.meta(), offset, limit);

        Ok((ParquetReader::open(Arc::clone(&source), size, footer.meta(), &columns, plan, 1_024)?, source))
    }

    fn read(file: Vec<u8>, columns: &[&str]) -> Result<Vec<ArrayRef>, Error> {
        let (mut reader, _) = open(file, columns, 0, None)?;
        let (_, arrays) = reader.next().unwrap()?;

        assert!(reader.next().is_none());

        Ok(arrays)
    }

    fn int96(days_since_epoch: i64, nanos_of_day: i64) -> Int96 {
        let mut value = Int96::new();
        value.set_data(nanos_of_day as u32, (nanos_of_day >> 32) as u32, (days_since_epoch + 2_440_588) as u32);

        value
    }

    /// `ts` INT96 flat, `nanos` INT64 TIMESTAMP(NANOS), `st.ts` INT96 in a struct, `l` a list of INT96.
    fn int96_file() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type(
                "message s { optional int96 ts; optional int64 nanos (TIMESTAMP(NANOS,false)); \
                 optional group st { optional int96 ts; } \
                 optional group l (LIST) { repeated group list { optional int96 element; } } }",
            )
            .unwrap(),
        );
        let mut file = Vec::new();
        let mut writer = SerializedFileWriter::new(&mut file, schema, Arc::new(WriterProperties::builder().build())).unwrap();
        let mut group = writer.next_row_group().unwrap();
        let first = int96(-719_162, 1_000);
        let last = int96(-3_653, 86_399_999_999_999);

        let mut column = group.next_column().unwrap().unwrap();
        column.typed::<Int96Type>().write_batch(&[first, last], Some(&[1, 1]), None).unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column.typed::<Int64Type>().write_batch(&[1_500, -1_500], Some(&[1, 1]), None).unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column.typed::<Int96Type>().write_batch(&[first, last], Some(&[2, 2]), None).unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column.typed::<Int96Type>().write_batch(&[first, last], Some(&[3, 3]), Some(&[0, 0])).unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();

        file
    }

    fn micros(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<TimestampMicrosecondType>().iter().collect()
    }

    #[test]
    fn int96_leaves_are_read_as_exact_microseconds() {
        let arrays = read(int96_file(), &["ts", "nanos", "st", "l"]).unwrap();
        let expected = vec![Some(-62_135_596_799_999_999), Some(-315_532_800_000_001)];

        assert_eq!(micros(&arrays[0]), expected);
        assert_eq!(micros(&arrays[1]), vec![Some(1), Some(-2)]);
        assert_eq!(micros(arrays[2].as_struct().column(0)), expected);
        assert_eq!(micros(arrays[3].as_list::<i32>().values()), expected);
    }

    #[test]
    fn int96_is_not_hinted_without_int96_leaves() {
        let schema = parse_message_type("message s { optional int64 nanos (TIMESTAMP(NANOS,false)); }").unwrap();
        let descr = SchemaDescriptor::new(Arc::new(schema));
        let inferred = Arc::new(Schema::new(vec![Field::new(
            "nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        assert!(int96_hint(&inferred, &descr).is_none());
    }

    fn struct_file() -> Vec<u8> {
        let children = Fields::from(vec![
            Field::new("int64", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let structure = StructArray::new(
            children.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let list = ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![Some(vec![Some(1)]); 3]);
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for _ in 0..3 {
            map.keys().append_value("k");
            map.values().append_value(1);
            map.append(true).unwrap();
        }
        let map = map.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("structure", DataType::Struct(children), true),
            Field::new("list", list.data_type().clone(), true),
            Field::new("map", map.data_type().clone(), true),
        ]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, Arc::clone(&schema), None).unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![Arc::new(structure), Arc::new(list), Arc::new(map)]).unwrap())
            .unwrap();
        writer.close().unwrap();

        file
    }

    #[test]
    fn a_struct_path_reads_the_child_null_under_a_null_struct() {
        let arrays = read(struct_file(), &["structure.int64", "structure.name"]).unwrap();

        assert_eq!(
            arrays[0].as_primitive::<arrow_array::types::Int64Type>().iter().collect::<Vec<_>>(),
            vec![Some(1), None, None]
        );
        assert_eq!(
            arrays[1].as_binary::<i32>().iter().collect::<Vec<_>>(),
            vec![Some(&b"a"[..]), None, Some(&b"c"[..])]
        );
    }

    #[test]
    fn a_struct_path_is_named_as_asked() {
        let (reader, _) = open(struct_file(), &["structure.name"], 0, None).unwrap();

        assert_eq!(reader.fields()[0].name(), "structure.name");
        assert!(reader.fields()[0].is_nullable());
        assert_eq!(reader.types(), &[DataType::Binary]);
    }

    #[test]
    fn a_path_into_a_list_or_a_map_is_refused() {
        for path in ["list.list.element", "list.item", "map.key_value.key", "map.entries"] {
            assert!(matches!(
                read(struct_file(), &[path]),
                Err(Error::Unsupported { column, parquet }) if column == path && parquet == "a path into LIST/MAP"
            ));
        }
    }

    #[test]
    fn a_path_that_names_nothing_is_missing() {
        for path in ["structure.missing", "missing.int64", "structure.int64.deeper"] {
            assert!(matches!(read(struct_file(), &[path]), Err(Error::MissingColumn(column)) if column == path));
        }
    }

    /// Columns `a`, `b`, `c` (Int64) in four row groups of 100 rows.
    fn four_groups() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let column = || Arc::new(Int64Array::from_iter_values(0..400)) as ArrayRef;
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut file,
            Arc::clone(&schema),
            Some(WriterProperties::builder().set_max_row_group_row_count(Some(100)).build()),
        )
        .unwrap();
        writer.write(&RecordBatch::try_new(schema, vec![column(), column(), column()]).unwrap()).unwrap();
        writer.close().unwrap();

        file
    }

    /// The compressed bytes of `columns`' chunks in `groups`.
    fn chunk_bytes(file: &[u8], groups: &[usize], columns: &[usize]) -> usize {
        let meta = Footer::read(&Bytes::from(file.to_vec())).unwrap();

        groups
            .iter()
            .flat_map(|group| columns.iter().map(move |column| (*group, *column)))
            .map(|(group, column)| meta.meta().row_group(group).column(column).byte_range().1 as usize)
            .sum()
    }

    fn drained(mut reader: ParquetReader) -> usize {
        let mut rows = 0;

        while let Some(batch) = reader.next() {
            rows += batch.unwrap().0;
        }

        rows
    }

    #[test]
    fn a_full_read_fetches_each_row_group_once() {
        let file = four_groups();
        let (reader, source) = open(file.clone(), &["a", "b", "c"], 0, None).unwrap();

        assert_eq!(drained(reader), 400);
        assert_eq!(source.counted(), (4, chunk_bytes(&file, &[0, 1, 2, 3], &[0, 1, 2])));
    }

    #[test]
    fn a_projected_read_fetches_only_the_projected_chunks() {
        let file = four_groups();
        let (reader, source) = open(file.clone(), &["b"], 0, None).unwrap();

        assert_eq!(drained(reader), 400);
        assert_eq!(source.counted(), (4, chunk_bytes(&file, &[0, 1, 2, 3], &[1])));
    }

    #[test]
    fn an_offset_read_fetches_only_the_planned_row_group_and_the_page_index() {
        let file = four_groups();
        let (reader, source) = open(file.clone(), &["a", "c"], 250, Some(10)).unwrap();
        let meta = Footer::read(&Bytes::from(file.clone())).unwrap();
        let index = meta
            .meta()
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .map(|chunk| {
                chunk.column_index_length().unwrap_or(0) as usize + chunk.offset_index_length().unwrap_or(0) as usize
            })
            .sum::<usize>();

        assert_eq!(drained(reader), 10);

        let (reads, bytes) = source.counted();

        assert!(reads <= 3, "{reads} reads");
        assert!(bytes >= chunk_bytes(&file, &[2], &[0, 2]), "{bytes} bytes");
        assert!(bytes <= chunk_bytes(&file, &[2], &[0, 1, 2]) + index, "{bytes} bytes");
    }

    #[test]
    fn plan_past_the_end_reads_nothing() {
        assert_eq!(plan(65_536, 10), (vec![], 0));
        assert_eq!(plan(0, 0), (vec![], 0));
    }
}
