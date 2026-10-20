//! Row groups of a Parquet file as canonical arrays: only the groups a read plan selects rows of are decoded, and only
//! the byte ranges arrow-rs's push decoder asks for are read - the selected pages when a plan skips rows inside a
//! group, whole projected chunks otherwise. Every projected column is checked against the cast table before the first
//! batch.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{make_array, Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder, RowGroupSelection};
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, Type};
use parquet::file::metadata::page_index::PageIndexBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::index_reader::decode_offset_index;
use parquet::schema::types::SchemaDescriptor;
use parquet::DecodeResult;

use crate::parquet::canonical::{canonical, canonical_type};
use crate::parquet::error::Error;
use crate::parquet::filter::{PlanPredicate, Raw};
use crate::parquet::source::{exact, ReadAt};

/// The row groups holding a selected row, each with its row-group-local selection - `None` when every row of the
/// group is selected - the row filter evaluated on the selected rows, and the rows returned at most, counted after it.
#[derive(Debug, PartialEq)]
pub struct ReadPlan {
    pub selections: Vec<RowGroupSelection>,
    pub filter: Option<Raw>,
    pub limit: Option<usize>,
}

impl ReadPlan {
    /// `selectors`: (row count, skip) over every row of the file, `filter` the row filter and `limit` the rows returned
    /// at most, as `ReadPlanConverter::toExtension()` sends them.
    pub fn from_selection(
        meta: &ParquetMetaData,
        selectors: Vec<(usize, bool)>,
        filter: Option<Raw>,
        limit: Option<usize>,
    ) -> Result<Self, Error> {
        let total = selectors
            .iter()
            .try_fold(0usize, |total, (count, _)| total.checked_add(*count))
            .ok_or_else(|| Error::Plan("A read plan's row counts overflow".to_string()))?;
        let rows = meta
            .row_groups()
            .iter()
            .try_fold(0usize, |rows, group| {
                rows.checked_add(usize::try_from(group.num_rows()).ok()?)
            })
            .ok_or_else(|| {
                Error::NotParquet("Invalid Parquet file. The row groups' row counts overflow".to_string())
            })?;

        if total != rows {
            return Err(Error::Plan(format!(
                "A read plan over {total} rows cannot read a file of {rows} rows"
            )));
        }

        let mut selections = Vec::new();
        let mut selectors = selectors.into_iter().filter(|(count, _)| *count > 0);
        let mut current = selectors.next();

        for (index, group) in meta.row_groups().iter().enumerate() {
            let mut remaining = group.num_rows() as usize;
            let mut kept = Vec::new();

            while remaining > 0 {
                // the totals are equal: a selector is left while a group has rows
                let Some((count, skip)) = current.as_mut() else {
                    break;
                };
                let taken = (*count).min(remaining);
                kept.push(if *skip {
                    RowSelector::skip(taken)
                } else {
                    RowSelector::select(taken)
                });
                *count -= taken;
                remaining -= taken;

                if *count == 0 {
                    current = selectors.next();
                }
            }

            if kept.iter().all(|selector| !selector.skip) {
                selections.push(RowGroupSelection::new(index, None));
            } else if kept.iter().any(|selector| !selector.skip) {
                selections.push(RowGroupSelection::new(index, Some(RowSelection::from(kept))));
            }
        }

        Ok(Self {
            selections,
            filter,
            limit,
        })
    }

    pub fn row_groups(&self) -> Vec<usize> {
        self.selections.iter().map(RowGroupSelection::row_group_index).collect()
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

        let path = name.split('.').map(str::to_string).collect::<Vec<_>>();
        let (field, nullable) = struct_field(schema, &path).map_err(|parent| match parent.map(Field::data_type) {
            Some(DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) | DataType::Map(_, _)) => {
                Error::Unsupported {
                    column: name.to_string(),
                    parquet: "a path into LIST/MAP".to_string(),
                }
            }
            _ => Error::MissingColumn(name.to_string()),
        })?;

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
    decoder: ParquetPushDecoder,
    source: Arc<dyn ReadAt + Send + Sync>,
    /// Each column's field as the file stores it, named as it was asked for.
    fields: Vec<FieldRef>,
    /// `canonical_type()` of each of `fields`.
    types: Vec<DataType>,
    /// Where each of `fields` sits in the projected batches: its root, then the struct children down to it.
    positions: Vec<(usize, Vec<String>)>,
}

impl ParquetReader {
    /// `source`: the file's bytes (`PhpStream`); `meta`: its footer without page indexes, which `plan` was built from. Offset indexes are loaded for the projected and predicate leaves of the groups the plan reads in part, and of every planned group when the plan has a row filter.
    pub fn open<R: ReadAt + Send + Sync + 'static>(
        source: Arc<R>,
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
        let schema = ArrowReaderMetadata::try_new(Arc::clone(meta), options.clone())?
            .schema()
            .clone();
        let projected = columns
            .iter()
            .map(|name| Projected::resolve(&schema, name))
            .collect::<Result<Vec<_>, _>>()?;
        let fields = projected
            .iter()
            .map(|column| Arc::clone(&column.field))
            .collect::<Vec<_>>();
        let types = fields
            .iter()
            .map(|field| canonical_type(field))
            .collect::<Result<Vec<_>, _>>()?;

        let descr = meta.file_metadata().schema_descr();
        let predicates = match &plan.filter {
            Some(filter) => PlanPredicate::conjuncts(filter, &schema, descr)?,
            None => Vec::new(),
        };
        let predicated = predicates
            .iter()
            .flat_map(|predicate| predicate.leaves().iter().copied())
            .collect::<Vec<_>>();

        if let Some(codec) = unsupported_codec(meta, &plan.row_groups(), &projected, &predicated) {
            return Err(codec);
        }

        let source: Arc<dyn ReadAt + Send + Sync> = source;
        let leaves = projected_leaves(descr, &projected);
        let fetched = (0..descr.num_columns())
            .filter(|leaf| leaves.contains(leaf) || predicated.contains(leaf))
            .collect::<Vec<_>>();
        let meta = ArrowReaderMetadata::try_new(with_offset_index(source.as_ref(), meta, &plan, &fetched)?, options)?;
        let mask = ProjectionMask::leaves(meta.metadata().file_metadata().schema_descr(), leaves);
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(meta)
            .with_row_group_selections(plan.selections)
            .with_projection(mask)
            .with_batch_size(batch_size);

        if !predicates.is_empty() {
            builder = builder.with_row_filter(RowFilter::new(
                predicates
                    .into_iter()
                    .map(|predicate| Box::new(predicate) as Box<dyn ArrowPredicate>)
                    .collect(),
            ));
        }

        if let Some(limit) = plan.limit {
            builder = builder.with_limit(limit);
        }

        let decoder = builder.build()?;
        let positions = positions(&schema, &projected)?;

        Ok(Self {
            decoder,
            source,
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
        let batch = loop {
            match self.decoder.try_decode() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    if let Err(error) = read_ranges(self.source.as_ref(), ranges)
                        .and_then(|(ranges, bytes)| Ok(self.decoder.push_ranges(ranges, bytes)?))
                    {
                        return Some(Err(error));
                    }
                }
                Ok(DecodeResult::Data(batch)) => break batch,
                Ok(DecodeResult::Finished) => return None,
                Err(error) => return Some(Err(error.into())),
            }
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

/// The bytes of `ranges`, read adjacent runs at a time and handed back per range asked: the decoder releases a pushed
/// buffer only when its range is one it asked for.
fn read_ranges(source: &dyn ReadAt, ranges: Vec<Range<u64>>) -> Result<(Vec<Range<u64>>, Vec<bytes::Bytes>), Error> {
    let reads = coalesced(ranges.clone())
        .into_iter()
        .map(|read| Ok((read.start, exact(source, read.start, (read.end - read.start) as usize)?)))
        .collect::<Result<Vec<_>, Error>>()?;
    let bytes = ranges
        .iter()
        .map(|range| {
            let (start, bytes) = &reads[reads.partition_point(|(start, _)| *start <= range.start) - 1];

            bytes.slice((range.start - start) as usize..(range.end - start) as usize)
        })
        .collect();

    Ok((ranges, bytes))
}

/// `meta` with the offset index of each fetched leaf of the row groups `plan` reads in part, or of every planned group
/// when a row filter can narrow it, where the selected pages are worth finding; every other chunk is fetched whole, as
/// without an index.
fn with_offset_index(
    source: &dyn ReadAt,
    meta: &Arc<ParquetMetaData>,
    plan: &ReadPlan,
    leaves: &[usize],
) -> Result<Arc<ParquetMetaData>, Error> {
    let indexed = plan
        .selections
        .iter()
        .filter(|group| plan.filter.is_some() || group.selection().is_some())
        .flat_map(|group| leaves.iter().map(move |leaf| (group.row_group_index(), *leaf)))
        .filter_map(|(group, leaf)| Some((group, leaf, meta.row_group(group).column(leaf).offset_index_range()?)))
        .collect::<Vec<_>>();

    if indexed.is_empty() {
        return Ok(Arc::clone(meta));
    }

    let (_, bytes) = read_ranges(source, indexed.iter().map(|(_, _, range)| range.clone()).collect())?;
    let mut page_index =
        PageIndexBuilder::new(meta.num_row_groups(), meta.file_metadata().schema_descr().num_columns());

    for ((group, leaf, _), bytes) in indexed.into_iter().zip(bytes) {
        page_index.put_offset_index(decode_offset_index(&bytes)?, group, leaf);
    }

    Ok(Arc::new(
        ParquetMetaData::clone(meta)
            .into_builder()
            .set_page_index(Some(Arc::new(page_index.build())))
            .build(),
    ))
}

/// The parquet leaves a projected column reads, in the file's order.
fn projected_leaves(descr: &SchemaDescriptor, projected: &[Projected]) -> Vec<usize> {
    (0..descr.num_columns())
        .filter(|leaf| {
            projected
                .iter()
                .any(|column| column.reads_leaf(descr.column(*leaf).path().parts()))
        })
        .collect()
}

/// Where each projected column sits in a batch, which holds the roots with a projected leaf in the file's order: its
/// root, then the struct children down to it.
fn positions(schema: &Schema, projected: &[Projected]) -> Result<Vec<(usize, Vec<String>)>, Error> {
    let roots = schema
        .fields()
        .iter()
        .map(|field| field.name())
        .filter(|name| projected.iter().any(|column| &column.path[0] == *name))
        .collect::<Vec<_>>();

    projected
        .iter()
        .map(|column| {
            let root = roots
                .iter()
                .position(|name| *name == &column.path[0])
                .ok_or_else(|| Error::MissingColumn(column.field.name().clone()))?;

            Ok((root, column.path[1..].to_vec()))
        })
        .collect()
}

/// Sorted ranges with every run of adjacent or overlapping ones merged.
fn coalesced(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());

    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.end >= range.start => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }

    merged
}

/// The struct child at `path` under `schema`'s roots and whether it or a struct above it is nullable; `Err` holds the
/// field a segment is no child of, `None` when no root is named `path[0]`.
pub fn struct_field<'a>(schema: &'a Schema, path: &[String]) -> Result<(&'a Field, bool), Option<&'a Field>> {
    let mut field = schema.field_with_name(&path[0]).map_err(|_| None)?;
    let mut nullable = field.is_nullable();

    for name in &path[1..] {
        field = match field.data_type() {
            DataType::Struct(children) => children.find(name).ok_or(Some(field))?.1,
            _ => return Err(Some(field)),
        };
        nullable |= field.is_nullable();
    }

    Ok((field, nullable))
}

/// The struct child at `path` under `array`, with the nulls of it and of every struct above it.
pub fn struct_child(array: &ArrayRef, path: &[String]) -> Result<(ArrayRef, Option<NullBuffer>), Error> {
    let mut array = Arc::clone(array);
    let mut nulls = array.nulls().cloned();

    for name in path {
        let child = Arc::clone(
            array
                .as_struct()
                .column_by_name(name)
                .ok_or_else(|| Error::MissingColumn(name.clone()))?,
        );
        nulls = NullBuffer::union(nulls.as_ref(), child.nulls());
        array = child;
    }

    Ok((array, nulls))
}

/// The struct child at `path` under `array`, null wherever a struct above it is.
fn child(array: &ArrayRef, path: &[String]) -> Result<ArrayRef, Error> {
    let (child, nulls) = struct_child(array, path)?;

    // a union only adds nulls: as many as the child's own means none was added
    if nulls.as_ref().map(NullBuffer::null_count) == child.nulls().map(NullBuffer::null_count) {
        return Ok(child);
    }

    // SAFETY: the union of the structs' and the child's null buffers has the child's length, and nulling a slot keeps
    // every other buffer of the child valid
    Ok(make_array(unsafe {
        child.to_data().into_builder().nulls(nulls).build_unchecked()
    }))
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

/// A codec the parquet crate is built without, in a projected or `predicated` leaf's chunk of the planned row groups.
fn unsupported_codec(
    meta: &ParquetMetaData,
    row_groups: &[usize],
    columns: &[Projected],
    predicated: &[usize],
) -> Option<Error> {
    row_groups.iter().find_map(|group| {
        meta.row_group(*group)
            .columns()
            .iter()
            .enumerate()
            .find_map(|(leaf, chunk)| {
                let column = match columns
                    .iter()
                    .find(|column| column.reads_leaf(chunk.column_path().parts()))
                {
                    Some(column) => column.field.name().clone(),
                    None if predicated.contains(&leaf) => chunk.column_path().string(),
                    None => return None,
                };

                (chunk.compression() == Compression::LZO).then(|| Error::Unsupported {
                    column,
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
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::arrow::push_decoder::RowGroupSelection;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::data_type::{Int64Type, Int96, Int96Type};
    use parquet::file::metadata::{FileMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{coalesced, int96_hint, unsupported_codec, ParquetReader, Projected, ReadPlan};
    use crate::parquet::error::Error;
    use crate::parquet::filter::Raw;
    use crate::parquet::footer::Footer;
    use crate::parquet::source::fake::Counting;

    /// Three row groups of 100 rows.
    fn three_groups() -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut file,
            Arc::clone(&schema),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(100))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..300))]).unwrap())
            .unwrap();
        writer.close().unwrap();

        ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(file))
            .unwrap()
    }

    #[test]
    fn a_selection_keeps_the_groups_it_selects_rows_of_each_with_its_local_selection() {
        let plan =
            ReadPlan::from_selection(&three_groups(), vec![(150, true), (20, false), (130, true)], None, None).unwrap();

        assert_eq!(
            plan.selections,
            vec![RowGroupSelection::new(
                1,
                Some(RowSelection::from(vec![
                    RowSelector::skip(50),
                    RowSelector::select(20),
                    RowSelector::skip(30)
                ]))
            )]
        );
    }

    #[test]
    fn a_group_with_no_selected_row_is_dropped_and_a_whole_group_has_no_selection() {
        let plan = ReadPlan::from_selection(
            &three_groups(),
            vec![
                (10, true),
                (90, false),
                (100, true),
                (50, false),
                (0, false),
                (50, true),
            ],
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            plan.selections,
            vec![
                RowGroupSelection::new(
                    0,
                    Some(RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(90)]))
                ),
                RowGroupSelection::new(
                    2,
                    Some(RowSelection::from(vec![RowSelector::select(50), RowSelector::skip(50)]))
                ),
            ]
        );
        assert_eq!(
            ReadPlan::from_selection(&three_groups(), vec![(100, true), (200, false)], None, None)
                .unwrap()
                .selections,
            vec![RowGroupSelection::new(1, None), RowGroupSelection::new(2, None)]
        );
    }

    #[test]
    fn a_selection_of_no_row_reads_no_group() {
        let plan = ReadPlan::from_selection(&three_groups(), vec![(300, true)], None, None).unwrap();

        assert!(plan.selections.is_empty());
    }

    #[test]
    fn a_selection_that_does_not_cover_the_file_is_refused() {
        for selectors in [vec![(299, false)], vec![(300, false), (1, true)], vec![]] {
            assert!(matches!(
                ReadPlan::from_selection(&three_groups(), selectors, None, None),
                Err(Error::Plan(message)) if message.ends_with("cannot read a file of 300 rows")
            ));
        }
    }

    #[test]
    fn selector_counts_that_overflow_are_refused() {
        assert!(matches!(
            ReadPlan::from_selection(&three_groups(), vec![(usize::MAX, false), (301, true)], None, None),
            Err(Error::Plan(message)) if message == "A read plan's row counts overflow"
        ));
    }

    /// `three_groups()` whose file metadata claims `num_rows` rows.
    fn claiming(num_rows: i64) -> ParquetMetaData {
        let meta = three_groups();
        let file = meta.file_metadata();

        ParquetMetaData::new(
            FileMetaData::new(
                file.version(),
                num_rows,
                file.created_by().map(str::to_string),
                file.key_value_metadata().cloned(),
                file.schema_descr_ptr(),
                file.column_orders().cloned(),
            ),
            meta.row_groups().to_vec(),
        )
    }

    #[test]
    fn a_plan_covers_the_row_groups_rows_whatever_the_file_row_count_claims() {
        for claimed in [200, 400] {
            assert_eq!(
                ReadPlan::from_selection(&claiming(claimed), vec![(300, false)], None, None)
                    .unwrap()
                    .row_groups(),
                vec![0, 1, 2]
            );

            for selectors in [vec![(claimed as usize, false)], vec![(299, false)], vec![(301, false)]] {
                assert!(matches!(
                    ReadPlan::from_selection(&claiming(claimed), selectors, None, None),
                    Err(Error::Plan(message)) if message.ends_with("cannot read a file of 300 rows")
                ));
            }
        }
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
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(StringArray::from(vec!["a"])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        let mut builder = ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(file))
            .unwrap()
            .into_builder();
        let row_groups = builder
            .take_row_groups()
            .into_iter()
            .map(|group| {
                let columns = group
                    .columns()
                    .iter()
                    .map(|chunk| match chunk.column_path().string().as_str() {
                        "name" => chunk
                            .clone()
                            .into_builder()
                            .set_compression(Compression::LZO)
                            .build()
                            .unwrap(),
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
            unsupported_codec(&meta, &[0], &[column("id"), column("name")], &[]),
            Some(Error::Unsupported { column, parquet }) if column == "name" && parquet == "LZO"
        ));
        assert!(unsupported_codec(&meta, &[0], &[column("id")], &[]).is_none());
    }

    fn open(
        file: Vec<u8>,
        columns: &[&str],
        selectors: Option<Vec<(usize, bool)>>,
    ) -> Result<(ParquetReader, Arc<Counting>), Error> {
        let footer = Footer::read(&Bytes::from(file.clone()))?;
        let source = Arc::new(Counting::new(file));
        let columns = columns.iter().map(|column| column.to_string()).collect::<Vec<_>>();
        let rows = footer.meta().file_metadata().num_rows() as usize;
        let plan = ReadPlan::from_selection(footer.meta(), selectors.unwrap_or(vec![(rows, false)]), None, None)?;

        Ok((
            ParquetReader::open(Arc::clone(&source), footer.meta(), &columns, plan, 1_024)?,
            source,
        ))
    }

    fn read(file: Vec<u8>, columns: &[&str]) -> Result<Vec<ArrayRef>, Error> {
        let (mut reader, _) = open(file, columns, None)?;
        let (_, arrays) = reader.next().unwrap()?;

        assert!(reader.next().is_none());

        Ok(arrays)
    }

    fn int96(days_since_epoch: i64, nanos_of_day: i64) -> Int96 {
        let mut value = Int96::new();
        value.set_data(
            nanos_of_day as u32,
            (nanos_of_day >> 32) as u32,
            (days_since_epoch + 2_440_588) as u32,
        );

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
        let mut writer =
            SerializedFileWriter::new(&mut file, schema, Arc::new(WriterProperties::builder().build())).unwrap();
        let mut group = writer.next_row_group().unwrap();
        let first = int96(-719_162, 1_000);
        let last = int96(-3_653, 86_399_999_999_999);

        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int96Type>()
            .write_batch(&[first, last], Some(&[1, 1]), None)
            .unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int64Type>()
            .write_batch(&[1_500, -1_500], Some(&[1, 1]), None)
            .unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int96Type>()
            .write_batch(&[first, last], Some(&[2, 2]), None)
            .unwrap();
        column.close().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int96Type>()
            .write_batch(&[first, last], Some(&[3, 3]), Some(&[0, 0]))
            .unwrap();
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
            arrays[0]
                .as_primitive::<arrow_array::types::Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), None, None]
        );
        assert_eq!(
            arrays[1].as_binary::<i64>().iter().collect::<Vec<_>>(),
            vec![Some(&b"a"[..]), None, Some(&b"c"[..])]
        );
    }

    #[test]
    fn a_struct_path_is_named_as_asked() {
        let (reader, _) = open(struct_file(), &["structure.name"], None).unwrap();

        assert_eq!(reader.fields()[0].name(), "structure.name");
        assert!(reader.fields()[0].is_nullable());
        assert_eq!(reader.types(), &[DataType::LargeBinary]);
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

    /// `a >= 150` read for `b`, from `selectors`, `limit` rows at most.
    fn filtered(selectors: Vec<(usize, bool)>, limit: Option<usize>) -> (Vec<i64>, Arc<Counting>) {
        let file = four_groups();
        let footer = Footer::read(&Bytes::from(file.clone())).unwrap();
        let source = Arc::new(Counting::new(file));
        let bytes = |value: &str| Raw::Bytes(value.as_bytes().to_vec());
        let filter = Raw::Map(vec![
            (b"leaf".to_vec(), bytes("intervals")),
            (b"path".to_vec(), Raw::List(vec![bytes("a")])),
            (
                b"intervals".to_vec(),
                Raw::List(vec![Raw::List(vec![
                    Raw::Int(150),
                    Raw::Bool(true),
                    Raw::Null,
                    Raw::Bool(false),
                ])]),
            ),
        ]);
        let plan = ReadPlan::from_selection(footer.meta(), selectors, Some(filter), limit).unwrap();
        let mut reader =
            ParquetReader::open(Arc::clone(&source), footer.meta(), &["b".to_string()], plan, 1_024).unwrap();
        let mut values = Vec::new();

        while let Some(batch) = reader.next() {
            values.extend(
                batch.unwrap().1[0]
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values()
                    .iter(),
            );
        }

        (values, source)
    }

    #[test]
    fn a_row_filter_reads_only_the_matching_rows_of_the_unprojected_predicate_column() {
        let (values, _) = filtered(vec![(400, false)], None);

        assert_eq!(values, (150..400).collect::<Vec<_>>());
    }

    #[test]
    fn a_row_filter_loads_the_offset_index_of_every_planned_group_and_skips_the_projection_of_a_group_it_empties() {
        let (_, source) = filtered(vec![(400, false)], None);
        let file = four_groups();

        assert_eq!(
            source.counted().1,
            offset_index_bytes(&file, &[(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1), (3, 0), (3, 1)])
                + chunk_bytes(&file, &[0, 1, 2, 3], &[0])
                + chunk_bytes(&file, &[1, 2, 3], &[1])
        );
    }

    #[test]
    fn a_limit_counts_the_rows_the_filter_keeps_and_no_page_of_the_next_group_is_fetched() {
        let (values, limited) = filtered(vec![(400, false)], Some(60));
        let (_, three_groups) = filtered(vec![(300, false), (100, true)], None);

        assert_eq!(values, (150..210).collect::<Vec<_>>());
        // the offset index of the last group is loaded with the plan, none of its pages
        assert_eq!(
            limited.counted().1,
            three_groups.counted().1 + offset_index_bytes(&four_groups(), &[(3, 0), (3, 1)])
        );
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
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(100))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![column(), column(), column()]).unwrap())
            .unwrap();
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
        let (reader, source) = open(file.clone(), &["a", "b", "c"], None).unwrap();

        assert_eq!(drained(reader), 400);
        assert_eq!(source.counted(), (4, chunk_bytes(&file, &[0, 1, 2, 3], &[0, 1, 2])));
    }

    #[test]
    fn a_projected_read_fetches_only_the_projected_chunks() {
        let file = four_groups();
        let (reader, source) = open(file.clone(), &["b"], None).unwrap();

        assert_eq!(drained(reader), 400);
        assert_eq!(source.counted(), (4, chunk_bytes(&file, &[0, 1, 2, 3], &[1])));
    }

    #[test]
    fn a_window_read_fetches_the_offset_index_and_the_projected_chunks_of_its_row_group() {
        let file = four_groups();
        let (reader, source) = open(
            file.clone(),
            &["a", "c"],
            Some(vec![(250, true), (10, false), (140, true)]),
        )
        .unwrap();

        assert_eq!(drained(reader), 10);
        assert_eq!(
            source.counted().1,
            offset_index_bytes(&file, &[(2, 0), (2, 2)]) + chunk_bytes(&file, &[2], &[0, 2])
        );
    }

    /// The bytes of the offset indexes of the given (group, leaf) chunks.
    fn offset_index_bytes(file: &[u8], chunks: &[(usize, usize)]) -> usize {
        let footer = Footer::read(&Bytes::from(file.to_vec())).unwrap();

        chunks
            .iter()
            .map(|(group, leaf)| {
                footer
                    .meta()
                    .row_group(*group)
                    .column(*leaf)
                    .offset_index_length()
                    .unwrap() as usize
            })
            .sum()
    }

    /// `s` (Utf8 "v{i % 5}", dictionary encoded) in groups of 100 rows, pages of 10 rows.
    fn paged_groups(groups: usize) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut file,
            Arc::clone(&schema),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(100))
                    .set_data_page_row_count_limit(10)
                    .set_write_batch_size(10)
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from_iter_values(
                        (0..groups * 100).map(|i| format!("v{}", i % 5)),
                    ))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        file
    }

    /// Per (group, pages), the group's offset index, its dictionary page and those data pages.
    fn page_bytes(file: &[u8], pages: &[(usize, &[usize])]) -> usize {
        let meta = ParquetMetaDataReader::new()
            .with_offset_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&Bytes::from(file.to_vec()))
            .unwrap();

        offset_index_bytes(file, &pages.iter().map(|(group, _)| (*group, 0)).collect::<Vec<_>>())
            + pages
                .iter()
                .map(|(group, pages)| {
                    let locations = meta
                        .page_index()
                        .unwrap()
                        .offset_index(*group, 0)
                        .unwrap()
                        .page_locations();
                    let dictionary = locations[0].offset - meta.row_group(*group).column(0).byte_range().0 as i64;

                    dictionary as usize
                        + pages
                            .iter()
                            .map(|page| locations[*page].compressed_page_size as usize)
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    fn strings(mut reader: ParquetReader) -> Vec<String> {
        let mut values = Vec::new();

        while let Some(batch) = reader.next() {
            let (_, arrays) = batch.unwrap();
            values.extend(
                arrays[0]
                    .as_binary::<i64>()
                    .iter()
                    .map(|value| String::from_utf8(value.unwrap().to_vec()).unwrap()),
            );
        }

        values
    }

    #[test]
    fn a_selection_inside_one_group_fetches_only_the_selected_pages_and_the_dictionary() {
        let file = paged_groups(1);
        let (reader, source) = open(file.clone(), &["s"], Some(vec![(35, true), (10, false), (55, true)])).unwrap();

        assert_eq!(
            strings(reader),
            (35..45).map(|i| format!("v{}", i % 5)).collect::<Vec<_>>()
        );
        assert_eq!(source.counted().1, page_bytes(&file, &[(0, &[3, 4])]));
    }

    #[test]
    fn a_selection_spanning_groups_fetches_the_selected_pages_of_each() {
        let file = paged_groups(3);
        let (reader, source) = open(file.clone(), &["s"], Some(vec![(95, true), (10, false), (195, true)])).unwrap();

        assert_eq!(
            strings(reader),
            (95..105).map(|i| format!("v{}", i % 5)).collect::<Vec<_>>()
        );
        assert_eq!(source.counted().1, page_bytes(&file, &[(0, &[9]), (1, &[0])]));
    }

    #[test]
    fn an_empty_selection_fetches_nothing() {
        let file = paged_groups(3);
        let (reader, source) = open(file, &["s"], Some(vec![(300, true)])).unwrap();

        assert_eq!(drained(reader), 0);
        assert_eq!(source.counted(), (0, 0));
    }

    #[test]
    fn a_pruned_plan_fetches_only_the_kept_groups_chunks() {
        let file = four_groups();
        let (reader, source) = open(
            file.clone(),
            &["a", "b", "c"],
            Some(vec![(100, true), (100, false), (100, true), (100, false)]),
        )
        .unwrap();

        assert_eq!(drained(reader), 200);
        assert_eq!(source.counted(), (2, chunk_bytes(&file, &[1, 3], &[0, 1, 2])));
    }

    #[test]
    fn adjacent_ranges_are_coalesced() {
        assert_eq!(coalesced(vec![10..20, 0..10, 30..40, 20..25]), vec![0..25, 30..40]);
        assert_eq!(coalesced(vec![0..10, 5..8, 8..12]), vec![0..12]);
        assert!(coalesced(vec![]).is_empty());
    }
}
