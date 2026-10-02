//! A planned read's column chunks, fetched a row group at a time: the first page request inside a planned row group
//! reads every projected column chunk of that group - adjacent chunks in one read - and every later page header and
//! page body of the group is a slice of those buffers. arrow-rs's sync page reader asks for each page header
//! separately (`get_read`), so answering from the stream per request would read the file many times over.
//!
//! arrow-rs advances each column's pages on its own, so one batch may take column `a` into the next row group while
//! column `b` still reads the previous one: a group's buffers are dropped once every projected column has moved past
//! it. Memory: the projected compressed chunks of the row groups one batch spans - the bound arrow-rs's async reader
//! and DuckDB accept, row groups being the writer's unit.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};

use crate::parquet::source::{exact, ReadAt};

/// A column chunk of the file and, when the read plans it, where: its planned group, its projected column, and the
/// coalesced read of that group holding it.
struct Chunk {
    range: Range<u64>,
    planned: Option<(usize, usize, usize)>,
}

#[derive(Default)]
struct Fetched {
    /// Planned group → its reads' bytes.
    buffers: BTreeMap<usize, Vec<Bytes>>,
    /// Per projected column, the planned group it reads now.
    progress: Vec<usize>,
}

pub struct ChunkFetch<R> {
    source: Arc<R>,
    size: u64,
    /// Per planned group, its projected chunks with every run of adjacent ones merged, in file order.
    reads: Vec<Vec<Range<u64>>>,
    /// Every column chunk of the file, by start: page requests find theirs by binary search.
    chunks: Vec<Chunk>,
    fetched: Mutex<Fetched>,
}

impl<R: ReadAt> ChunkFetch<R> {
    /// `row_groups`: the planned groups; `leaves`: the projected leaf columns.
    pub fn new(source: Arc<R>, size: u64, meta: &ParquetMetaData, row_groups: &[usize], leaves: &[usize]) -> Self {
        let range = |(start, length): (u64, u64)| start..start + length;
        let planned = row_groups
            .iter()
            .map(|group| {
                let columns = meta.row_group(*group).columns();

                leaves.iter().map(|leaf| range(columns[*leaf].byte_range())).collect()
            })
            .collect();
        let mut is_planned = vec![false; meta.num_row_groups()];
        let mut is_projected = vec![false; meta.file_metadata().schema_descr().num_columns()];
        row_groups.iter().for_each(|group| is_planned[*group] = true);
        leaves.iter().for_each(|leaf| is_projected[*leaf] = true);
        let (is_planned, is_projected) = (&is_planned, &is_projected);
        let unplanned = meta
            .row_groups()
            .iter()
            .enumerate()
            .flat_map(|(group, columns)| {
                columns
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(move |(leaf, _)| !is_planned[group] || !is_projected[*leaf])
                    .map(|(_, chunk)| range(chunk.byte_range()))
            })
            .collect();

        Self::from_ranges(source, size, planned, unplanned)
    }

    /// `planned`: per planned group, its projected chunks, by projected column; `unplanned`: every other chunk.
    fn from_ranges(source: Arc<R>, size: u64, planned: Vec<Vec<Range<u64>>>, unplanned: Vec<Range<u64>>) -> Self {
        let columns = planned.first().map_or(0, Vec::len);
        let reads = planned
            .iter()
            .map(|chunks| coalesced(chunks.clone()))
            .collect::<Vec<_>>();
        let mut chunks = unplanned
            .into_iter()
            .map(|range| Chunk { range, planned: None })
            .chain(planned.into_iter().enumerate().flat_map(|(group, chunks)| {
                let reads = &reads[group];

                chunks
                    .into_iter()
                    .enumerate()
                    .map(move |(column, range)| {
                        let read = reads
                            .iter()
                            .position(|read| read.start <= range.start && range.end <= read.end);

                        Chunk {
                            planned: read.map(|read| (group, column, read)),
                            range,
                        }
                    })
                    .collect::<Vec<_>>()
            }))
            .collect::<Vec<_>>();
        chunks.sort_by_key(|chunk| chunk.range.start);

        Self {
            source,
            size,
            reads,
            chunks,
            fetched: Mutex::new(Fetched {
                buffers: BTreeMap::new(),
                progress: vec![0; columns],
            }),
        }
    }

    /// The column chunk holding `offset`.
    fn chunk_at(&self, offset: u64) -> Option<&Chunk> {
        let after = self.chunks.partition_point(|chunk| chunk.range.start <= offset);

        after
            .checked_sub(1)
            .map(|index| &self.chunks[index])
            .filter(|chunk| offset < chunk.range.end)
    }

    /// The fetched bytes of [start, end) when a planned column chunk holds it, fetching its row group first.
    fn planned(&self, start: u64, end: u64) -> Result<Option<Bytes>, ParquetError> {
        let Some((group, column, read)) = self.chunk_at(start).and_then(|chunk| chunk.planned) else {
            return Ok(None);
        };
        let range = &self.reads[group][read];

        if end > range.end {
            return Ok(None);
        }

        let mut fetched = self
            .fetched
            .lock()
            .map_err(|_| ParquetError::General("chunk fetch poisoned".into()))?;

        if let Entry::Vacant(entry) = fetched.buffers.entry(group) {
            entry.insert(
                self.reads[group]
                    .iter()
                    .map(|range| exact(self.source.as_ref(), range.start, (range.end - range.start) as usize))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        let bytes = fetched.buffers[&group][read].slice((start - range.start) as usize..(end - range.start) as usize);
        fetched.progress[column] = group;
        let slowest = fetched.progress.iter().copied().min().unwrap_or(group);
        fetched.buffers.retain(|fetched_group, _| *fetched_group >= slowest);

        Ok(Some(bytes))
    }
}

/// Sorted ranges with every run of adjacent ones merged.
fn coalesced(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());

    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.end == range.start => last.end = range.end,
            _ => merged.push(range),
        }
    }

    merged
}

impl<R> Length for ChunkFetch<R> {
    fn len(&self) -> u64 {
        self.size
    }
}

impl<R: ReadAt + Send + Sync> ChunkReader for ChunkFetch<R> {
    type T = Reader<Bytes>;

    /// A page header: the rest of its planned read; outside the plan, one exact read to the end of the column chunk
    /// holding `start` (of the file when none does).
    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let end = self.chunk_at(start).map_or(self.size, |chunk| match chunk.planned {
            Some((group, _, read)) => self.reads[group][read].end,
            None => chunk.range.end,
        });

        Ok(match self.planned(start, end)? {
            Some(bytes) => bytes,
            None => exact(self.source.as_ref(), start, (end - start) as usize)?,
        }
        .reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        match self.planned(start, start + length as u64)? {
            Some(bytes) => Ok(bytes),
            None => exact(self.source.as_ref(), start, length),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::ops::Range;
    use std::sync::Arc;

    use parquet::file::reader::ChunkReader;

    use super::{coalesced, ChunkFetch};
    use crate::parquet::source::fake::Counting;

    #[test]
    fn adjacent_ranges_are_coalesced() {
        assert_eq!(coalesced(vec![10..20, 0..10, 30..40, 20..25]), vec![0..25, 30..40]);
        assert!(coalesced(vec![]).is_empty());
    }

    fn fetch(
        source: &Arc<Counting>,
        planned: Vec<Vec<Range<u64>>>,
        unplanned: Vec<Range<u64>>,
    ) -> ChunkFetch<Counting> {
        ChunkFetch::from_ranges(Arc::clone(source), source.data.len() as u64, planned, unplanned)
    }

    #[test]
    fn a_read_outside_the_plan_is_one_exact_read_to_the_end_of_its_chunk() {
        let source = Arc::new(Counting::new((0..100).collect()));
        let fetch = fetch(&source, vec![vec![10..20]], vec![30..35, 40..60]);
        let mut rest = Vec::new();

        fetch.get_read(45).unwrap().read_to_end(&mut rest).unwrap();

        assert_eq!(rest, (45..60).collect::<Vec<u8>>());
        assert_eq!(source.counted(), (1, 15));

        rest.clear();
        fetch.get_read(90).unwrap().read_to_end(&mut rest).unwrap();

        assert_eq!(rest, (90..100).collect::<Vec<u8>>());
        assert_eq!(fetch.get_bytes(70, 2).unwrap().to_vec(), vec![70, 71]);
        assert_eq!(source.counted(), (3, 27));
    }

    #[test]
    fn a_planned_group_is_fetched_once_in_coalesced_reads() {
        let source = Arc::new(Counting::new((0..100).collect()));
        let fetch = fetch(&source, vec![vec![0..10, 10..15, 20..30]], vec![]);
        let mut header = Vec::new();

        assert_eq!(fetch.get_bytes(22, 3).unwrap().to_vec(), vec![22, 23, 24]);
        fetch.get_read(12).unwrap().read_to_end(&mut header).unwrap();
        assert_eq!(fetch.get_bytes(1, 2).unwrap().to_vec(), vec![1, 2]);

        assert_eq!(header, (12..15).collect::<Vec<u8>>());
        assert_eq!(source.counted(), (2, 25));
    }

    #[test]
    fn a_group_is_dropped_once_every_column_moved_past_it() {
        let source = Arc::new(Counting::new((0..100).collect()));
        let fetch = fetch(&source, vec![vec![0..10, 20..30], vec![50..60, 70..80]], vec![]);
        let groups = |fetch: &ChunkFetch<Counting>| {
            fetch
                .fetched
                .lock()
                .unwrap()
                .buffers
                .keys()
                .copied()
                .collect::<Vec<_>>()
        };

        fetch.get_bytes(0, 1).unwrap();
        fetch.get_bytes(50, 1).unwrap();
        assert_eq!(groups(&fetch), vec![0, 1]);

        fetch.get_bytes(21, 1).unwrap();
        assert_eq!(groups(&fetch), vec![0, 1]);

        fetch.get_bytes(71, 1).unwrap();
        assert_eq!(groups(&fetch), vec![1]);
        assert_eq!(source.counted(), (4, 40));
    }

    #[test]
    fn page_requests_find_their_chunk_among_many() {
        let source = Arc::new(Counting::new((0..=255).cycle().take(40_000).collect()));
        let planned = (0..100).map(|group| vec![group * 400..group * 400 + 100, group * 400 + 200..group * 400 + 300]);
        let fetch = fetch(
            &source,
            planned.collect(),
            (0..100).map(|group| group * 400 + 100..group * 400 + 200).collect(),
        );

        assert_eq!(
            fetch.get_bytes(99 * 400 + 250, 2).unwrap().to_vec(),
            source.data[39_850..39_852].to_vec()
        );
        assert_eq!(source.counted(), (2, 200));

        let mut header = Vec::new();
        fetch
            .get_read(50 * 400 + 150)
            .unwrap()
            .read_to_end(&mut header)
            .unwrap();

        assert_eq!(header, source.data[20_150..20_200].to_vec());
        assert_eq!(source.counted(), (3, 250));
    }
}
