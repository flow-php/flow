//! CSV records and fields exactly as the PHP path produces them: `CSVLineReader` assembles a record
//! (a `\n` ends it only outside an enclosure, the record is `rtrim($buffer, "\r\n")`, the first one
//! loses its BOM) and `str_getcsv()` splits it - a port of `php_fgetcsv()` (`ext/standard/file.c`).

use std::ops::Range;

use memchr::{memchr, memchr2};

pub struct Dialect {
    pub separator: u8,
    pub enclosure: u8,
    pub escape: Option<u8>,
}

/// One record's fields as ranges of `bytes`; no field at all is a blank line (`str_getcsv('')` is `[null]`).
#[derive(Default)]
pub struct Record {
    bytes: Vec<u8>,
    fields: Vec<Range<usize>>,
}

pub enum Field<'a> {
    Value(&'a [u8]),
    Null,
    Missing,
}

impl Record {
    pub fn len(&self) -> usize {
        self.fields.len().max(1)
    }

    pub fn field(&self, index: usize) -> Field<'_> {
        match self.fields.get(index) {
            Some(range) => Field::Value(&self.bytes[range.clone()]),
            None if index == 0 && self.fields.is_empty() => Field::Null,
            None => Field::Missing,
        }
    }
}

/// Where the record scan stands after the last byte it looked at.
#[derive(Clone, Copy)]
enum Scan {
    FieldStart,
    Unenclosed,
    Enclosed,
    Escaped,
    ClosingOrDoubled,
}

const BOMS: [&[u8]; 5] = [
    b"\xEF\xBB\xBF",
    b"\xFF\xFE\x00\x00",
    b"\x00\x00\xFE\xFF",
    b"\xFF\xFE",
    b"\xFE\xFF",
];

/// Resumable across chunk boundaries: `feed` appends bytes, `next` fills the next complete record or returns
/// `false` while it is not yet closed. A record left open at EOF is emitted as-is (PHP's `unterminated` case).
pub struct Tokenizer {
    dialect: Dialect,
    remove_bom: bool,
    buffer: Vec<u8>,
    record_start: usize,
    scanned: usize,
    scan: Scan,
    finished: bool,
    first_record: bool,
    last_record_bytes: usize,
}

impl Tokenizer {
    pub fn new(dialect: Dialect, remove_bom: bool) -> Self {
        Self {
            dialect,
            remove_bom,
            buffer: Vec::new(),
            record_start: 0,
            scanned: 0,
            scan: Scan::FieldStart,
            finished: false,
            first_record: true,
            last_record_bytes: 0,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        if self.record_start > 0 {
            self.buffer.drain(..self.record_start);
            self.scanned -= self.record_start;
            self.record_start = 0;
        }

        self.buffer.extend_from_slice(chunk);
    }

    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// The bytes the last record `next` returned took in the stream: line ending and BOM included.
    pub fn last_record_bytes(&self) -> usize {
        self.last_record_bytes
    }

    pub fn next(&mut self, record: &mut Record) -> bool {
        let Some(range) = self.next_record() else {
            return false;
        };

        self.split(range, record);

        true
    }

    /// The next complete record as a range of `buffer`, after `rtrim("\r\n")` and the BOM strip.
    fn next_record(&mut self) -> Option<Range<usize>> {
        let mut record = match self.scan_to_record_end() {
            Some(newline) => {
                let range = self.record_start..newline;
                self.record_start = newline + 1;
                self.scanned = self.record_start;
                self.scan = Scan::FieldStart;

                range
            }
            None if self.finished && self.record_start < self.buffer.len() => {
                let range = self.record_start..self.buffer.len();
                self.record_start = self.buffer.len();
                self.scanned = self.record_start;
                self.scan = Scan::FieldStart;

                range
            }
            None => return None,
        };

        self.last_record_bytes = self.record_start - record.start;

        while record.end > record.start && matches!(self.buffer[record.end - 1], b'\r' | b'\n') {
            record.end -= 1;
        }

        if self.remove_bom && self.first_record {
            if let Some(bom) = BOMS.iter().find(|bom| self.buffer[record.clone()].starts_with(bom)) {
                record.start += bom.len();
            }
        }

        self.first_record = false;

        Some(record)
    }

    /// Advances the scan over unseen bytes; returns the position of the `\n` that ends the record.
    fn scan_to_record_end(&mut self) -> Option<usize> {
        let Dialect { separator, enclosure, escape } = self.dialect;
        let bytes = &self.buffer;
        let mut position = self.scanned;
        let mut scan = self.scan;

        let found = loop {
            if position >= bytes.len() {
                break None;
            }

            match scan {
                Scan::FieldStart => {
                    let byte = bytes[position];

                    if byte == b'\n' {
                        break Some(position);
                    }

                    scan = if byte == separator {
                        Scan::FieldStart
                    } else if byte == enclosure {
                        Scan::Enclosed
                    } else if is_space(byte) {
                        Scan::FieldStart
                    } else {
                        Scan::Unenclosed
                    };
                    position += 1;
                }
                Scan::Unenclosed => match memchr2(separator, b'\n', &bytes[position..]) {
                    Some(offset) if bytes[position + offset] == b'\n' => break Some(position + offset),
                    Some(offset) => {
                        position += offset + 1;
                        scan = Scan::FieldStart;
                    }
                    None => position = bytes.len(),
                },
                Scan::Enclosed => {
                    let special = match escape {
                        Some(escape) if escape != enclosure => memchr2(enclosure, escape, &bytes[position..]),
                        _ => memchr(enclosure, &bytes[position..]),
                    };

                    match special {
                        Some(offset) => {
                            scan = if bytes[position + offset] == enclosure {
                                Scan::ClosingOrDoubled
                            } else {
                                Scan::Escaped
                            };
                            position += offset + 1;
                        }
                        None => position = bytes.len(),
                    }
                }
                Scan::Escaped => {
                    scan = Scan::Enclosed;
                    position += 1;
                }
                Scan::ClosingOrDoubled => {
                    if bytes[position] == enclosure {
                        scan = Scan::Enclosed;
                        position += 1;
                    } else {
                        scan = Scan::Unenclosed;
                    }
                }
            }
        };

        self.scanned = position;
        self.scan = scan;

        found
    }

    /// `str_getcsv($record, ...)` into `record`.
    fn split(&self, range: Range<usize>, record: &mut Record) {
        let Dialect { separator, enclosure, escape } = self.dialect;
        let escape = escape.filter(|escape| *escape != enclosure);
        let line = &self.buffer[range];
        // next_record() already stripped every trailing \r and \n, so php_fgetcsv()'s line end is always empty here
        let limit = line.len();

        record.bytes.clear();
        record.fields.clear();

        if limit == 0 {
            return;
        }

        let mut position = 0;

        loop {
            let field_start = record.bytes.len();

            if position < limit {
                let mut skipped = position;

                while skipped < limit && line[skipped] != separator && is_space(line[skipped]) {
                    skipped += 1;
                }

                if skipped < limit && line[skipped] == enclosure {
                    position = skipped;
                }
            }

            let delimiter = if position < limit && line[position] == enclosure {
                position += 1;
                let mut hunk = position;

                let unterminated = loop {
                    let Some(offset) = (match escape {
                        Some(escape) => memchr2(enclosure, escape, &line[position..limit]),
                        None => memchr(enclosure, &line[position..limit]),
                    }) else {
                        break true;
                    };

                    position += offset;

                    if line[position] != enclosure {
                        position += 2;

                        if position > limit {
                            break true;
                        }

                        continue;
                    }

                    if position + 1 < limit && line[position + 1] == enclosure {
                        record.bytes.extend_from_slice(&line[hunk..=position]);
                        position += 2;
                        hunk = position;

                        continue;
                    }

                    record.bytes.extend_from_slice(&line[hunk..position]);
                    position += 1;
                    hunk = position;

                    break false;
                };

                if unterminated {
                    record.bytes.extend_from_slice(&line[hunk..limit]);
                    hunk = limit;
                    position = limit;
                }

                let delimiter = memchr(separator, &line[position..limit]).map(|offset| position + offset);
                let end = delimiter.unwrap_or(limit);

                if hunk < end {
                    record.bytes.extend_from_slice(&line[hunk..end]);
                }

                delimiter
            } else {
                let delimiter = memchr(separator, &line[position..limit]).map(|offset| position + offset);

                record.bytes.extend_from_slice(&line[position..delimiter.unwrap_or(limit)]);

                let trimmed = field_start + trailing_line_end(&record.bytes[field_start..]);
                record.bytes.truncate(trimmed);

                delimiter
            };

            record.fields.push(field_start..record.bytes.len());

            match delimiter {
                Some(delimiter) => position = delimiter + 1,
                None => break,
            }
        }
    }
}

/// `isspace()` in the C locale.
pub fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r')
}

/// `php_fgetcsv_lookup_trailing_spaces()`: the length without one trailing `\r\n`, `\n` or `\r`.
fn trailing_line_end(bytes: &[u8]) -> usize {
    match bytes {
        [.., b'\r', b'\n'] => bytes.len() - 2,
        [.., b'\n'] | [.., b'\r'] => bytes.len() - 1,
        _ => bytes.len(),
    }
}
