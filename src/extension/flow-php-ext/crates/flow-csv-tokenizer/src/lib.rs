//! CSV records and fields exactly as the PHP path produces them: a one-pass port of `php_fgetcsv()`.

use std::io;
use std::ops::Range;

use memchr::{memchr, memchr2, memchr_iter};

pub struct Dialect {
    pub separator: u8,
    pub enclosure: u8,
    pub escape: Option<u8>,
}

/// What a byte is at the start of a field: `php_fgetcsv()` ends the record at `\n`, closes an empty field at the
/// separator, opens an enclosure after any spaces, and reads anything else as an unenclosed field.
#[derive(Clone, Copy)]
enum Class {
    Plain,
    Separator,
    Enclosure,
    Newline,
    Space,
}

struct Table([Class; 256]);

impl Table {
    fn new(dialect: &Dialect) -> Self {
        let mut classes = [Class::Plain; 256];

        for byte in [b' ', b'\t', 0x0B, 0x0C, b'\r'] {
            classes[usize::from(byte)] = Class::Space;
        }

        classes[usize::from(dialect.enclosure)] = Class::Enclosure;
        classes[usize::from(dialect.separator)] = Class::Separator;
        classes[usize::from(b'\n')] = Class::Newline;

        Self(classes)
    }

    fn of(&self, byte: u8) -> Class {
        self.0[usize::from(byte)]
    }
}

/// Where a field's bytes live: in the read buffer, from the record's start, or rebuilt in the record.
enum Span {
    Buffer(Range<usize>),
    Unescaped(Range<usize>),
}

/// One record's fields, valid until the next `feed()` or `fill_with()`; no field is a blank line.
#[derive(Default)]
pub struct Record {
    /// Where the record begins in the tokenizer's buffer.
    start: usize,
    /// Where it ends, its line ending excluded.
    end: usize,
    fields: Vec<Span>,
    unescaped: Vec<u8>,
}

pub enum Field<'a> {
    Value(&'a [u8]),
    Null,
    Missing,
}

impl Record {
    /// Never 0: `str_getcsv('')` is `[null]`, one field.
    pub fn field_count(&self) -> usize {
        self.fields.len().max(1)
    }

    /// An empty line: no field at all.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    fn clear(&mut self) {
        self.fields.clear();
        self.unescaped.clear();
    }
}

/// An open enclosure, offsets from the record's start: `content` is the first byte after the opening enclosure,
/// `hunk` the first byte not yet copied into the record, `rebuilt` where the copy began once a doubled enclosure
/// forced one.
#[derive(Clone, Copy)]
struct Quoted {
    content: usize,
    hunk: usize,
    rebuilt: Option<usize>,
}

/// A closed enclosure: the bytes between `close` and the field's end follow the value, as `php_fgetcsv()` keeps them.
#[derive(Clone, Copy)]
struct Closed {
    quoted: Quoted,
    close: usize,
}

/// Where the scan stands after the last byte it looked at; offsets from the record's start.
#[derive(Clone, Copy)]
enum Scan {
    FieldStart { field: usize },
    Unenclosed { field: usize },
    Enclosed(Quoted),
    Escaped(Quoted),
    ClosingOrDoubled(Quoted),
    AfterClose(Closed),
}

const BOMS: [&[u8]; 5] = [
    b"\xEF\xBB\xBF",
    b"\xFF\xFE\x00\x00",
    b"\x00\x00\xFE\xFF",
    b"\xFF\xFE",
    b"\xFE\xFF",
];

/// Resumable across chunk boundaries; a record left open at EOF is emitted as-is.
pub struct Tokenizer {
    dialect: Dialect,
    table: Table,
    /// The first record's BOM is still to be looked for.
    strip_bom: bool,
    buffer: Vec<u8>,
    record_start: usize,
    /// How far the scan looked.
    scanned: usize,
    scan: Scan,
    /// The record being scanned, spans from `record_start`.
    open: Record,
    finished: bool,
    first_record: bool,
    /// The BOM the first record loses once its end is found, counted in its bytes.
    bom: usize,
    last_record_bytes: usize,
    /// The newlines before `counted`, the buffer position `line()` last counted to, compacted bytes included.
    newlines: u64,
    counted: usize,
}

impl Tokenizer {
    pub fn new(dialect: Dialect, remove_bom: bool) -> Self {
        Self {
            table: Table::new(&dialect),
            dialect,
            strip_bom: remove_bom,
            buffer: Vec::new(),
            record_start: 0,
            scanned: 0,
            scan: Scan::FieldStart { field: 0 },
            open: Record::default(),
            finished: false,
            first_record: true,
            bom: 0,
            last_record_bytes: 0,
            newlines: 0,
            counted: 0,
        }
    }

    /// Drops the records already yielded: only the open record's bytes move, so the spans of the last yielded record
    /// are no longer valid.
    fn compact(&mut self) {
        if self.record_start > 0 {
            self.count_newlines_to(self.record_start);
            self.counted -= self.record_start;
            self.buffer.drain(..self.record_start);
            self.scanned -= self.record_start;
            self.record_start = 0;
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.compact();
        self.buffer.extend_from_slice(chunk);
    }

    /// Up to `len` bytes read straight into the buffer by `read`, which returns how many it wrote (0 at EOF).
    pub fn fill_with(&mut self, len: usize, read: impl FnOnce(&mut [u8]) -> io::Result<usize>) -> io::Result<usize> {
        self.compact();

        let start = self.buffer.len();
        self.buffer.resize(start + len, 0);
        let got = read(&mut self.buffer[start..]);
        self.buffer.truncate(start + got.as_ref().copied().unwrap_or(0));

        got
    }

    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// The bytes the last record `next` returned took in the stream: line ending and BOM included.
    pub fn last_record_bytes(&self) -> usize {
        self.last_record_bytes
    }

    /// The field `index` of `record`; valid until the next `feed()` or `fill_with()`.
    pub fn field<'a>(&'a self, record: &'a Record, index: usize) -> Field<'a> {
        match record.fields.get(index) {
            Some(Span::Buffer(range)) => {
                Field::Value(&self.buffer[record.start + range.start..record.start + range.end])
            }
            Some(Span::Unescaped(range)) => Field::Value(&record.unescaped[range.clone()]),
            None if index == 0 && record.fields.is_empty() => Field::Null,
            None => Field::Missing,
        }
    }

    /// The 1-based line `record` starts on, valid until the next `feed()`.
    pub fn line(&mut self, record: &Record) -> u64 {
        self.count_newlines_to(record.start);

        1 + self.newlines
    }

    fn count_newlines_to(&mut self, position: usize) {
        if position > self.counted {
            self.newlines += memchr_iter(b'\n', &self.buffer[self.counted..position]).count() as u64;
            self.counted = position;
        }
    }

    /// The record's bytes as the stream holds them, its line ending excluded; valid until the next `feed()`.
    pub fn text<'a>(&'a self, record: &'a Record) -> &'a [u8] {
        &self.buffer[record.start..record.end.max(record.start)]
    }

    pub fn next(&mut self, record: &mut Record) -> bool {
        if self.strip_bom && !self.find_bom() {
            return false;
        }

        let end = match self.scan(self.buffer.len(), true) {
            Some(newline) => Some((newline, true)),
            None if self.finished && self.record_start < self.buffer.len() => Some((self.buffer.len(), false)),
            None => None,
        };

        let Some((end, newline)) = end else {
            return false;
        };

        let start = self.record_start;

        if self.bom > 0 {
            // as the PHP path: the line was assembled with its BOM (whose first byte opens an unenclosed field), then
            // str_getcsv() split it without the BOM, every byte of it a field byte
            self.record_start += self.bom;
            self.scanned = self.record_start;
            self.scan = Scan::FieldStart { field: 0 };
            self.open.clear();
            self.scan(end, false);
        }

        self.close_record(end);
        self.last_record_bytes = end - start + usize::from(newline);
        self.open.start = self.record_start;
        std::mem::swap(record, &mut self.open);
        self.open.clear();
        self.record_start = end + usize::from(newline);
        self.scanned = self.record_start;
        self.scan = Scan::FieldStart { field: 0 };
        self.first_record = false;
        self.bom = 0;

        true
    }

    /// The first record's BOM measured; `false` while the buffered bytes could still be the start of a longer one.
    fn find_bom(&mut self) -> bool {
        let pending = &self.buffer[self.record_start..];

        if !self.finished
            && BOMS
                .iter()
                .any(|bom| bom.len() > pending.len() && bom.starts_with(pending))
        {
            return false;
        }

        self.bom = BOMS
            .iter()
            .find(|bom| pending.starts_with(bom))
            .map_or(0, |bom| bom.len());
        self.strip_bom = false;

        true
    }

    /// Scans unseen bytes up to `limit`; `Some` is the `\n` that ends the record when `newline_ends` is set.
    fn scan(&mut self, limit: usize, newline_ends: bool) -> Option<usize> {
        let Dialect {
            separator,
            enclosure,
            escape,
        } = self.dialect;
        let escape = escape.filter(|escape| *escape != enclosure);
        let base = self.record_start;
        let bytes = &self.buffer[..limit];
        let len = bytes.len();
        let open = &mut self.open;
        let mut position = self.scanned;
        let mut scan = self.scan;
        let field_end = |from: usize| match newline_ends {
            true => memchr2(separator, b'\n', &bytes[from..]),
            false => memchr(separator, &bytes[from..]),
        };

        let found = loop {
            if position >= len {
                break None;
            }

            match scan {
                Scan::FieldStart { field } => match self.table.of(bytes[position]) {
                    Class::Newline if newline_ends => break Some(position),
                    Class::Newline | Class::Space => position += 1,
                    Class::Separator => {
                        open.fields.push(unenclosed(bytes, base, field, position));
                        position += 1;
                        scan = Scan::FieldStart { field: position - base };
                    }
                    Class::Enclosure => {
                        position += 1;
                        scan = Scan::Enclosed(Quoted {
                            content: position - base,
                            hunk: position - base,
                            rebuilt: None,
                        });
                    }
                    Class::Plain => scan = Scan::Unenclosed { field },
                },
                Scan::Unenclosed { field } => match field_end(position) {
                    Some(offset) if bytes[position + offset] == b'\n' => break Some(position + offset),
                    Some(offset) => {
                        let end = position + offset;
                        open.fields.push(unenclosed(bytes, base, field, end));
                        position = end + 1;
                        scan = Scan::FieldStart { field: position - base };
                    }
                    None => position = len,
                },
                Scan::Enclosed(quoted) => {
                    let special = match escape {
                        Some(escape) => memchr2(enclosure, escape, &bytes[position..]),
                        None => memchr(enclosure, &bytes[position..]),
                    };

                    match special {
                        Some(offset) => {
                            let at = position + offset;
                            position = at + 1;
                            scan = if bytes[at] == enclosure {
                                Scan::ClosingOrDoubled(quoted)
                            } else {
                                Scan::Escaped(quoted)
                            };
                        }
                        None => position = len,
                    }
                }
                Scan::Escaped(quoted) => {
                    position += 1;
                    scan = Scan::Enclosed(quoted);
                }
                Scan::ClosingOrDoubled(mut quoted) => {
                    if bytes[position] == enclosure {
                        // a doubled enclosure keeps one: the hunk up to and including the first is copied
                        quoted.rebuilt.get_or_insert(open.unescaped.len());
                        open.unescaped.extend_from_slice(&bytes[base + quoted.hunk..position]);
                        position += 1;
                        quoted.hunk = position - base;
                        scan = Scan::Enclosed(quoted);
                    } else {
                        scan = Scan::AfterClose(Closed {
                            quoted,
                            close: position - 1 - base,
                        });
                    }
                }
                Scan::AfterClose(closed) => match field_end(position) {
                    Some(offset) if bytes[position + offset] == b'\n' => break Some(position + offset),
                    Some(offset) => {
                        let end = position + offset;
                        open.fields
                            .push(enclosed(&mut open.unescaped, bytes, base, closed, end - base));
                        position = end + 1;
                        scan = Scan::FieldStart { field: position - base };
                    }
                    None => position = len,
                },
            }
        };

        self.scanned = position;
        self.scan = scan;

        found
    }

    /// Closes the field the scan stands in at the record's end `end` (the `\n`, or the buffer's end at EOF), after
    /// `rtrim($record, "\r\n")`.
    fn close_record(&mut self, end: usize) {
        let base = self.record_start;
        let bytes = &self.buffer;
        let open = &mut self.open;
        let mut limit = end;

        while limit > base && matches!(bytes[limit - 1], b'\r' | b'\n') {
            limit -= 1;
        }

        open.end = limit;
        let limit = limit - base;

        match self.scan {
            Scan::FieldStart { field } => {
                if !(open.fields.is_empty() && limit == 0) {
                    open.fields
                        .push(unenclosed(bytes, base, field, base + limit.max(field)));
                }
            }
            Scan::Unenclosed { field } => open
                .fields
                .push(unenclosed(bytes, base, field, base + limit.max(field))),
            Scan::Enclosed(quoted) | Scan::Escaped(quoted) => {
                // unterminated: everything to the record's end is the value
                let limit = limit.max(quoted.hunk);

                open.fields.push(match quoted.rebuilt {
                    Some(start) => {
                        open.unescaped
                            .extend_from_slice(&bytes[base + quoted.hunk..base + limit]);

                        Span::Unescaped(start..open.unescaped.len())
                    }
                    None => Span::Buffer(quoted.content..limit),
                });
            }
            Scan::ClosingOrDoubled(quoted) => {
                let closed = Closed {
                    quoted,
                    close: self.scanned - 1 - base,
                };

                open.fields
                    .push(enclosed(&mut open.unescaped, bytes, base, closed, limit));
            }
            Scan::AfterClose(closed) => open
                .fields
                .push(enclosed(&mut open.unescaped, bytes, base, closed, limit)),
        }
    }
}

/// An unenclosed field `field..end` less one trailing `\r\n`, `\n` or `\r` (`php_fgetcsv_lookup_trailing_spaces()`).
fn unenclosed(bytes: &[u8], base: usize, field: usize, end: usize) -> Span {
    let trimmed = match &bytes[base + field..end] {
        [.., b'\r', b'\n'] => end - 2,
        [.., b'\n'] | [.., b'\r'] => end - 1,
        _ => end,
    };

    Span::Buffer(field..trimmed - base)
}

/// An enclosed field closed at `closed.close`, with the bytes up to `end` (from the record's start) after the closing
/// enclosure appended as they are; a slice of the buffer when nothing was rebuilt and nothing follows.
fn enclosed(unescaped: &mut Vec<u8>, bytes: &[u8], base: usize, closed: Closed, end: usize) -> Span {
    let Closed { quoted, close } = closed;
    let trailing = close + 1..end.max(close + 1);

    match quoted.rebuilt {
        None if trailing.is_empty() => Span::Buffer(quoted.content..close),
        rebuilt => {
            let start = rebuilt.unwrap_or(unescaped.len());

            unescaped.extend_from_slice(&bytes[base + quoted.hunk..base + close]);
            unescaped.extend_from_slice(&bytes[base + trailing.start..base + trailing.end]);

            Span::Unescaped(start..unescaped.len())
        }
    }
}

/// `isspace()` in the C locale.
pub fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r')
}

#[cfg(test)]
mod tests {
    use super::{Dialect, Field, Record, Tokenizer};

    fn dialect(separator: u8, enclosure: u8, escape: Option<u8>) -> Dialect {
        Dialect {
            separator,
            enclosure,
            escape,
        }
    }

    fn comma() -> Dialect {
        dialect(b',', b'"', Some(b'\\'))
    }

    /// Every record of `raw` fed in `chunk`-byte pieces, each field as `Some(bytes)`, a blank line as `[None]`.
    fn records(dialect: Dialect, remove_bom: bool, raw: &[u8], chunk: usize) -> Vec<Vec<Option<Vec<u8>>>> {
        let mut tokenizer = Tokenizer::new(dialect, remove_bom);
        let mut record = Record::default();
        let mut records = Vec::new();
        let mut drain = |tokenizer: &mut Tokenizer, records: &mut Vec<Vec<Option<Vec<u8>>>>| {
            while tokenizer.next(&mut record) {
                records.push(
                    (0..record.field_count())
                        .map(|index| match tokenizer.field(&record, index) {
                            Field::Value(value) => Some(value.to_vec()),
                            Field::Null => None,
                            Field::Missing => unreachable!("within field_count()"),
                        })
                        .collect(),
                );
            }
        };

        for piece in raw.chunks(chunk.max(1)) {
            tokenizer.feed(piece);
            drain(&mut tokenizer, &mut records);
        }

        tokenizer.finish();
        drain(&mut tokenizer, &mut records);

        records
    }

    fn same_at_every_chunk(dialect: impl Fn() -> Dialect, raw: &[u8]) -> Vec<Vec<Option<Vec<u8>>>> {
        let whole = records(dialect(), true, raw, raw.len().max(1));

        for chunk in [1, 2, 3, 7] {
            assert_eq!(records(dialect(), true, raw, chunk), whole, "chunk {chunk}");
        }

        whole
    }

    fn row(fields: &[&str]) -> Vec<Option<Vec<u8>>> {
        fields.iter().map(|field| Some(field.as_bytes().to_vec())).collect()
    }

    #[test]
    fn fields_are_slices_of_the_buffer_and_stay_valid_across_a_fill_boundary() {
        let raw = b"id,name\n1,\"multi\nline\"\n2,plain\n";
        let mut tokenizer = Tokenizer::new(comma(), false);
        let mut record = Record::default();
        let mut offset = 0;

        let mut fill = |tokenizer: &mut Tokenizer, len: usize| {
            tokenizer
                .fill_with(len, |buffer| {
                    let got = len.min(raw.len() - offset);
                    buffer[..got].copy_from_slice(&raw[offset..offset + got]);
                    offset += got;

                    Ok(got)
                })
                .unwrap()
        };

        assert_eq!(fill(&mut tokenizer, 12), 12);
        assert!(tokenizer.next(&mut record));
        assert!(matches!(tokenizer.field(&record, 1), Field::Value(b"name")));
        assert!(
            !tokenizer.next(&mut record),
            "the second record is open at the fill boundary"
        );

        assert_eq!(fill(&mut tokenizer, 100), raw.len() - 12);
        assert!(tokenizer.next(&mut record));
        assert!(matches!(tokenizer.field(&record, 1), Field::Value(b"multi\nline")));
        assert!(tokenizer.next(&mut record));
        assert!(matches!(tokenizer.field(&record, 1), Field::Value(b"plain")));
        assert_eq!(tokenizer.last_record_bytes(), 8);
        assert!(!tokenizer.next(&mut record));
        assert_eq!(fill(&mut tokenizer, 100), 0);
    }

    #[test]
    fn a_record_of_only_separators_is_empty_fields_and_a_blank_line_is_one_null() {
        assert_eq!(
            same_at_every_chunk(comma, b",,\n\n,\n"),
            vec![row(&["", "", ""]), vec![None], row(&["", ""])]
        );
    }

    #[test]
    fn a_doubled_enclosure_at_the_last_byte_of_a_buffer_keeps_one() {
        assert_eq!(
            same_at_every_chunk(comma, b"\"a\"\"b\",\"\"\"\",\"c\"\"\"\n"),
            vec![row(&["a\"b", "\"", "c\""])]
        );
    }

    #[test]
    fn an_escape_followed_by_the_buffer_end_keeps_the_escaped_byte() {
        assert_eq!(
            same_at_every_chunk(comma, b"\"a\\\"b\",\"c\\\",d\n\"e\\"),
            vec![row(&["a\\\"b", "c\\\",d\ne\\"])]
        );
        assert_eq!(
            same_at_every_chunk(|| dialect(b',', b'"', None), b"\"a\\\"b\"\n"),
            vec![row(&["a\\b\""])]
        );
    }

    #[test]
    fn a_crlf_split_across_buffers_is_stripped_and_a_trailing_cr_inside_a_field_is_trimmed_once() {
        assert_eq!(
            same_at_every_chunk(comma, b"a,b\r\nc\r,d\r\r\n\"e\"\r\n"),
            vec![row(&["a", "b"]), row(&["c", "d"]), row(&["e"])]
        );
        assert_eq!(
            same_at_every_chunk(comma, b"\"\" \"\",\"\",\t\r,\n\r\r,x\n"),
            vec![row(&[" \"\"", "", "\t", ""]), row(&["\r", "x"])]
        );
    }

    #[test]
    fn every_bom_is_stripped_from_the_first_record_only_and_counted_in_its_bytes() {
        // the line is assembled with the BOM in place, so the enclosure after it does not continue the line; the
        // fields are split without the BOM, so that enclosure opens an unterminated field
        assert_eq!(
            same_at_every_chunk(comma, b"\xEF\xBB\xBF\"a\nb\",c\n1,2\n"),
            vec![row(&["a"]), row(&["b\"", "c"]), row(&["1", "2"])]
        );
        assert_eq!(
            same_at_every_chunk(comma, b"\xEF\xBB\xBF\"a\",b\n1,2\n"),
            vec![row(&["a", "b"]), row(&["1", "2"])]
        );

        for bom in super::BOMS {
            let raw = [bom, b"a,b\n", bom, b"c\n"].concat();
            let expected = vec![row(&["a", "b"]), vec![Some([bom, b"c"].concat())]];

            assert_eq!(same_at_every_chunk(comma, &raw), expected, "{bom:?}");

            let mut tokenizer = Tokenizer::new(comma(), true);
            let mut record = Record::default();
            tokenizer.feed(&raw);
            assert!(tokenizer.next(&mut record));
            assert_eq!(tokenizer.last_record_bytes(), bom.len() + 4);
        }

        assert_eq!(
            records(comma(), false, b"\xEF\xBB\xBFa\n", 1),
            vec![vec![Some(b"\xEF\xBB\xBFa".to_vec())]]
        );
        assert_eq!(records(comma(), true, b"\xEF\xBB\xBF", 1), vec![vec![None]]);
        assert_eq!(records(comma(), true, b"\xFF\xFE", 1), vec![vec![None]]);
    }

    #[test]
    fn php_fgetcsv_shapes() {
        assert_eq!(
            same_at_every_chunk(comma, b"a, \"b\nc\",d\ne\n"),
            vec![row(&["a", "b\nc", "d"]), row(&["e"])]
        );
        assert_eq!(
            same_at_every_chunk(|| dialect(b'\t', b'"', Some(b'\\')), b"h1\th2\th3\na\t\t\"b\"\n"),
            vec![row(&["h1", "h2", "h3"]), row(&["a", "", "b"])]
        );
        assert_eq!(
            same_at_every_chunk(comma, b"\"a\" x,  \"b\"y\n"),
            vec![row(&["a x", "by"])]
        );
        assert_eq!(
            same_at_every_chunk(comma, b"\"open\nat eof"),
            vec![row(&["open\nat eof"])]
        );
        assert_eq!(same_at_every_chunk(comma, b"\"\"\n"), vec![row(&[""])]);
        assert_eq!(same_at_every_chunk(comma, b" \n"), vec![row(&[" "])]);
        assert_eq!(same_at_every_chunk(comma, b"a,\n"), vec![row(&["a", ""])]);
        assert_eq!(same_at_every_chunk(comma, b"\r\n"), vec![vec![None]]);
    }

    #[test]
    fn a_field_past_the_record_is_missing() {
        let mut tokenizer = Tokenizer::new(comma(), false);
        let mut record = Record::default();
        tokenizer.feed(b"a\n");

        assert!(tokenizer.next(&mut record));
        assert_eq!(record.field_count(), 1);
        assert!(matches!(tokenizer.field(&record, 0), Field::Value(b"a")));
        assert!(matches!(tokenizer.field(&record, 1), Field::Missing));
    }

    #[test]
    fn text_is_the_record_as_read_without_its_line_ending_and_a_blank_line_is_empty() {
        let mut tokenizer = Tokenizer::new(comma(), true);
        let mut record = Record::default();
        let mut seen = Vec::new();

        tokenizer.feed(b"\xEF\xBB\xBFa,\"b\r\nc\"\r\n\r\n4,5");
        tokenizer.finish();

        while tokenizer.next(&mut record) {
            seen.push((tokenizer.text(&record).to_vec(), record.is_empty()));
        }

        assert_eq!(
            seen,
            vec![
                (b"a,\"b\r\nc\"".to_vec(), false),
                (b"".to_vec(), true),
                (b"4,5".to_vec(), false),
            ]
        );
    }

    #[test]
    fn a_line_counts_every_newline_fed_before_the_record_compacted_or_not() {
        let mut tokenizer = Tokenizer::new(comma(), true);
        let mut record = Record::default();
        let mut lines = Vec::new();

        for chunk in [&b"\xEF\xBB\xBFa,\"b\r\nc\"\r\n\r"[..], b"\n4,5\n", b"6,7"] {
            tokenizer.feed(chunk);

            while tokenizer.next(&mut record) {
                lines.push(tokenizer.line(&record));
            }
        }

        tokenizer.finish();

        while tokenizer.next(&mut record) {
            lines.push(tokenizer.line(&record));
        }

        assert_eq!(lines, vec![1, 3, 4, 5]);
    }
}
