//! Row groups of a Parquet file as canonical arrays: only the groups an offset/limit window overlaps are decoded, and
//! every projected column is checked against the cast table before the first batch.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatchReader};
use arrow_schema::{DataType, FieldRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

use crate::parquet::canonical::{canonical, canonical_type};
use crate::parquet::error::Error;
use crate::parquet::source::PhpSource;

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

pub struct ParquetReader {
    batches: ParquetRecordBatchReader,
    fields: Vec<FieldRef>,
    /// `canonical_type()` of each of `fields`.
    types: Vec<DataType>,
    /// Where each of `fields` sits in the projected batches.
    positions: Vec<usize>,
}

impl ParquetReader {
    /// `meta`: the footer `plan` was built from (`PhpSource::metadata()`); the page index is read on top of it only
    /// when the plan skips rows inside its first row group.
    pub fn open(
        source: PhpSource,
        meta: ParquetMetaData,
        columns: &[String],
        plan: ReadPlan,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let meta = if plan.skip > 0 {
            let mut reader = ParquetMetaDataReader::new_with_metadata(meta).with_page_index_policy(PageIndexPolicy::Optional);
            reader.read_page_indexes(&source)?;
            reader.finish()?
        } else {
            meta
        };
        let meta = ArrowReaderMetadata::try_new(Arc::new(meta), ArrowReaderOptions::new())?;
        let schema = meta.schema();
        let roots = columns
            .iter()
            .map(|name| schema.index_of(name).map_err(|_| Error::MissingColumn(name.clone())))
            .collect::<Result<Vec<_>, _>>()?;
        let fields = roots.iter().map(|root| Arc::clone(&schema.fields()[*root])).collect::<Vec<_>>();
        let types = fields.iter().map(|field| canonical_type(field)).collect::<Result<Vec<_>, _>>()?;

        if let Some(codec) = unsupported_codec(meta.metadata(), &plan.row_groups, &fields) {
            return Err(codec);
        }

        let mask = ProjectionMask::roots(meta.metadata().file_metadata().schema_descr(), roots.iter().copied());
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(source, meta)
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
        let projected = batches.schema();
        let positions = columns
            .iter()
            .map(|name| projected.index_of(name).map_err(|_| Error::MissingColumn(name.clone())))
            .collect::<Result<Vec<_>, _>>()?;

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
                .map(|(field, position)| canonical(field, batch.column(*position)))
                .collect::<Result<Vec<_>, _>>()
                .map(|columns| (batch.num_rows(), columns)),
        )
    }
}

/// A codec the parquet crate is built without, in a projected column chunk of the planned row groups.
fn unsupported_codec(meta: &ParquetMetaData, row_groups: &[usize], fields: &[FieldRef]) -> Option<Error> {
    row_groups.iter().find_map(|group| {
        meta.row_group(*group).columns().iter().find_map(|chunk| {
            let root = chunk.column_path().parts().first()?;

            (chunk.compression() == Compression::LZO && fields.iter().any(|field| field.name() == root)).then(|| {
                Error::Unsupported {
                    column: root.clone(),
                    parquet: "LZO".to_string(),
                }
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, FieldRef, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;

    use super::{unsupported_codec, ReadPlan};
    use crate::parquet::error::Error;

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
        let name: FieldRef = Arc::new(Field::new("name", DataType::Utf8, true));
        let id: FieldRef = Arc::new(Field::new("id", DataType::Int64, false));

        assert!(matches!(
            unsupported_codec(&meta, &[0], &[Arc::clone(&id), name]),
            Some(Error::Unsupported { column, parquet }) if column == "name" && parquet == "LZO"
        ));
        assert!(unsupported_codec(&meta, &[0], &[id]).is_none());
    }

    #[test]
    fn plan_past_the_end_reads_nothing() {
        assert_eq!(plan(65_536, 10), (vec![], 0));
        assert_eq!(plan(0, 0), (vec![], 0));
    }
}
