//! `DateTimeInterface::format()` (ext/date `date_format()`) for the letters the native writers render, over an
//! instant in µs and the column zone. Any other letter makes the format unparseable: the column is rendered in PHP.

use ext_php_rs::exception::PhpException;

use crate::ctx::{self, call_method, ht_get, zval_long};
use crate::exception::ext_exception;
use crate::text::int;

pub const LETTERS: &[u8] = b"YymndjHGisuveTPpOZUc";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Literal(Vec<u8>),
    Letter(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Format(Vec<Part>);

impl Format {
    /// `None` when an unescaped ASCII letter is outside [`LETTERS`], or the format ends in a lone backslash.
    pub fn parse(format: &[u8]) -> Option<Self> {
        let mut parts = Vec::new();
        let mut literal = Vec::new();
        let mut bytes = format.iter();

        while let Some(byte) = bytes.next() {
            match byte {
                b'\\' => literal.push(*bytes.next()?),
                byte if byte.is_ascii_alphabetic() => {
                    if !LETTERS.contains(byte) {
                        return None;
                    }

                    if !literal.is_empty() {
                        parts.push(Part::Literal(std::mem::take(&mut literal)));
                    }

                    parts.push(Part::Letter(*byte));
                }
                byte => literal.push(*byte),
            }
        }

        if !literal.is_empty() {
            parts.push(Part::Literal(literal));
        }

        Some(Self(parts))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub from: i64,
    pub offset: i32,
    pub abbreviation: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Zone {
    Utc,
    Offset { seconds: i32, abbreviation: Vec<u8> },
    Named { id: Vec<u8>, transitions: Vec<Transition> },
}

impl Zone {
    /// `DateTimeType::zoneName()`: `UTC`, `+HH:MM` / `-HH:MM`, or an IANA id.
    pub fn of(zone_name: &[u8]) -> Self {
        if zone_name == b"UTC" {
            return Zone::Utc;
        }

        if let [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] = zone_name {
            if [h1, h2, m1, m2].iter().all(|digit| digit.is_ascii_digit()) {
                let digit = |byte: &u8| i32::from(byte - b'0');
                let seconds = (digit(h1) * 10 + digit(h2)) * 3_600 + (digit(m1) * 10 + digit(m2)) * 60;
                let seconds = if *sign == b'-' { -seconds } else { seconds };
                // timelib's abbreviation of a fixed offset: GMT±hhmm
                let mut abbreviation = b"GMT".to_vec();

                abbreviation.push(*sign);
                abbreviation.extend_from_slice(&[*h1, *h2, *m1, *m2]);

                return Zone::Offset { seconds, abbreviation };
            }
        }

        Zone::Named {
            id: zone_name.to_vec(),
            transitions: Vec::new(),
        }
    }

    /// A named zone reads `DateTimeZone::getTransitions($min, $max)`: the offset in force at `$min`, then every change
    /// up to `$max`. Once per batch per column.
    pub fn resolve(&mut self, min_second: i64, max_second: i64) -> Result<(), PhpException> {
        let Zone::Named { id, transitions } = self else {
            return Ok(());
        };

        let listed = call_method(&ctx::timezone(id)?, "getTransitions", &mut [zval_long(min_second), zval_long(max_second)])?;
        let listed = listed
            .array()
            .ok_or_else(|| ext_exception("flow_php expected DateTimeZone::getTransitions() to return an array"))?;

        transitions.clear();

        for transition in listed.values() {
            let field = |name: &[u8]| {
                transition
                    .array()
                    .and_then(|transition| ht_get(transition, name))
                    .ok_or_else(|| ext_exception("flow_php expected a time zone transition of ts, offset and abbr"))
            };

            transitions.push(Transition {
                from: field(b"ts")?.long().unwrap_or(min_second),
                offset: field(b"offset")?.long().unwrap_or(0) as i32,
                abbreviation: field(b"abbr")?.zend_str().map(|abbr| abbr.as_bytes().to_vec()).unwrap_or_default(),
            });
        }

        if transitions.is_empty() {
            return Err(ext_exception("flow_php expected DateTimeZone::getTransitions() to list the offset in force"));
        }

        Ok(())
    }

    /// The offset from UTC in seconds and the zone abbreviation at `second`.
    fn at(&self, second: i64) -> (i32, &[u8]) {
        match self {
            Zone::Utc => (0, b"UTC"),
            Zone::Offset { seconds, abbreviation } => (*seconds, abbreviation),
            Zone::Named { transitions, .. } => {
                let index = transitions.partition_point(|transition| transition.from <= second).saturating_sub(1);

                transitions.get(index).map_or((0, b"UTC"), |transition| (transition.offset, &transition.abbreviation))
            }
        }
    }
}

/// C's `%0{width}d`: zeros after the sign, the sign counted in the width.
fn padded(out: &mut Vec<u8>, value: i64, width: usize) {
    if width == 2 && (0..100).contains(&value) {
        out.extend_from_slice(&[b'0' + (value / 10) as u8, b'0' + (value % 10) as u8]);

        return;
    }

    if width == 4 && (1_000..10_000).contains(&value) {
        out.extend_from_slice(&[
            b'0' + (value / 1_000) as u8,
            b'0' + (value / 100 % 10) as u8,
            b'0' + (value / 10 % 10) as u8,
            b'0' + (value % 10) as u8,
        ]);

        return;
    }

    let mut buffer = [0u8; 20];
    let mut at = buffer.len();
    let mut rest = value.unsigned_abs();

    loop {
        at -= 1;
        buffer[at] = b'0' + (rest % 10) as u8;
        rest /= 10;

        if rest == 0 {
            break;
        }
    }

    let sign = usize::from(value < 0);

    if value < 0 {
        out.push(b'-');
    }

    out.resize(out.len() + width.saturating_sub(buffer.len() - at + sign), b'0');
    out.extend_from_slice(&buffer[at..]);
}

/// `±hh[:]mm` of an offset, as `P` / `O` / `c` / `e` write it.
fn offset_text(out: &mut Vec<u8>, offset: i32, colon: bool) {
    out.push(if offset < 0 { b'-' } else { b'+' });
    padded(out, i64::from((offset / 3_600).abs()), 2);

    if colon {
        out.push(b':');
    }

    padded(out, i64::from((offset % 3_600 / 60).abs()), 2);
}

/// The proleptic Gregorian date of a day count from 1970-01-01 (days-from-civil inverted), any year.
fn civil(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 };

    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
}

pub fn write(out: &mut Vec<u8>, format: &Format, micros: i64, zone: &Zone) {
    let second = micros.div_euclid(1_000_000);
    let micro = micros.rem_euclid(1_000_000);
    let (offset, abbreviation) = zone.at(second);
    let local = second + i64::from(offset);
    let (year, month, day) = civil(local.div_euclid(86_400));
    let of_day = local.rem_euclid(86_400);
    let (hour, minute, second_of_minute) = (of_day / 3_600, of_day / 60 % 60, of_day % 60);

    for part in &format.0 {
        let letter = match part {
            Part::Literal(bytes) => {
                out.extend_from_slice(bytes);

                continue;
            }
            Part::Letter(letter) => *letter,
        };

        match letter {
            b'Y' => {
                if year < 0 {
                    out.push(b'-');
                }

                padded(out, year.abs(), 4);
            }
            b'y' => padded(out, year % 100, 2),
            b'm' => padded(out, month, 2),
            b'n' => int(out, month),
            b'd' => padded(out, day, 2),
            b'j' => int(out, day),
            b'H' => padded(out, hour, 2),
            b'G' => int(out, hour),
            b'i' => padded(out, minute, 2),
            b's' => padded(out, second_of_minute, 2),
            b'u' => padded(out, micro, 6),
            b'v' => padded(out, micro / 1_000, 3),
            b'e' => match zone {
                Zone::Utc => out.extend_from_slice(b"UTC"),
                Zone::Offset { seconds, .. } => offset_text(out, *seconds, true),
                Zone::Named { id, .. } => out.extend_from_slice(id),
            },
            b'T' => out.extend_from_slice(abbreviation),
            b'p' if matches!(abbreviation, b"UTC" | b"Z" | b"GMT+0000") => out.push(b'Z'),
            b'P' | b'p' => offset_text(out, offset, true),
            b'O' => offset_text(out, offset, false),
            b'Z' => int(out, i64::from(offset)),
            b'U' => int(out, second),
            b'c' => {
                padded(out, year, 4);
                out.push(b'-');
                padded(out, month, 2);
                out.push(b'-');
                padded(out, day, 2);
                out.push(b'T');
                padded(out, hour, 2);
                out.push(b':');
                padded(out, minute, 2);
                out.push(b':');
                padded(out, second_of_minute, 2);
                offset_text(out, offset, true);
            }
            _ => unreachable!("Format::parse() keeps the rendered letters only"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{write, Format, Transition, Zone};

    fn formatted(format: &str, micros: i64, zone: &Zone) -> String {
        let mut out = Vec::new();

        write(&mut out, &Format::parse(format.as_bytes()).unwrap(), micros, zone);

        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_letter_outside_the_set_or_a_lone_backslash_is_not_parsed() {
        assert!(Format::parse(b"Y-m-d\\TH:i:s.uP").is_some());
        assert!(Format::parse(b"").is_some());
        assert!(Format::parse(b"D, d M Y").is_none());
        assert!(Format::parse(b"Y I").is_none());
        assert!(Format::parse(b"Y\\").is_none());
        assert!(Format::parse("Y ż \\D".as_bytes()).is_some());
    }

    /// `edge-facts.log`: format `Y|y|c|U|u|v|e|T|P|p|O|Z|m|n|d|j|H|G|i|s` in UTC at extreme years.
    #[test]
    fn every_letter_at_extreme_years_in_utc() {
        let format = "Y|y|c|U|u|v|e|T|P|p|O|Z|m|n|d|j|H|G|i|s";

        for (second, expected) in [
            (-62_198_755_200_i64, "-0001|-1|-001-01-01T00:00:00+00:00|-62198755200|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
            (-62_167_219_200, "0000|00|0000-01-01T00:00:00+00:00|-62167219200|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
            (-30_610_224_000, "1000|00|1000-01-01T00:00:00+00:00|-30610224000|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
            (-30_641_760_000, "0999|99|0999-01-01T00:00:00+00:00|-30641760000|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
            (253_402_300_800, "10000|00|10000-01-01T00:00:00+00:00|253402300800|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
            (3_093_527_980_800, "100000|00|100000-01-01T00:00:00+00:00|3093527980800|000000|000|UTC|UTC|+00:00|Z|+0000|0|01|1|01|1|00|0|00|00"),
        ] {
            assert_eq!(formatted(format, second * 1_000_000, &Zone::Utc), expected);
        }
    }

    #[test]
    fn micros_before_the_epoch_floor_to_the_second_below() {
        assert_eq!(formatted("Y-m-d H:i:s.u v U", -500_000, &Zone::Utc), "1969-12-31 23:59:59.500000 500 -1");
        assert_eq!(formatted("Y-m-d\\TH:i:s.u G\\h", 1_767_323_045_123_456, &Zone::Utc), "2026-01-02T03:04:05.123456 3h");
    }

    /// `edge-facts.log`: Europe/Warsaw across the 2026 gap and overlap, one transition list for the whole call.
    #[test]
    fn a_named_zone_crosses_its_transitions() {
        let warsaw = Zone::Named {
            id: b"Europe/Warsaw".to_vec(),
            transitions: vec![
                Transition { from: 1_774_745_999, offset: 3_600, abbreviation: b"CET".to_vec() },
                Transition { from: 1_774_746_000, offset: 7_200, abbreviation: b"CEST".to_vec() },
                Transition { from: 1_792_890_000, offset: 3_600, abbreviation: b"CET".to_vec() },
            ],
        };

        for (second, expected) in [
            (1_774_745_999_i64, "2026-03-29T01:59:59+01:00 CET Europe/Warsaw +01:00 3600"),
            (1_774_746_000, "2026-03-29T03:00:00+02:00 CEST Europe/Warsaw +02:00 7200"),
            (1_792_889_999, "2026-10-25T02:59:59+02:00 CEST Europe/Warsaw +02:00 7200"),
            (1_792_890_000, "2026-10-25T02:00:00+01:00 CET Europe/Warsaw +01:00 3600"),
        ] {
            assert_eq!(formatted("Y-m-d\\TH:i:sP T e p Z", second * 1_000_000, &warsaw), expected);
        }
    }

    /// `zone-transitions.log`: `+02:30` formats `e|T|P|O|Z|c` as `+02:30|GMT+0230|+02:30|+0230|9000|1970-01-01T02:30:00+02:30`.
    #[test]
    fn a_fixed_offset_zone() {
        assert_eq!(
            formatted("e|T|P|p|O|Z|c", 0, &Zone::of(b"+02:30")),
            "+02:30|GMT+0230|+02:30|+02:30|+0230|9000|1970-01-01T02:30:00+02:30"
        );
        assert_eq!(formatted("e|T|P|O|Z H:i", 0, &Zone::of(b"-05:00")), "-05:00|GMT-0500|-05:00|-0500|-18000 19:00");
    }

    #[test]
    fn zone_names() {
        assert_eq!(Zone::of(b"UTC"), Zone::Utc);
        assert!(matches!(Zone::of(b"Europe/Warsaw"), Zone::Named { .. }));
        assert!(matches!(Zone::of(b"+02:30"), Zone::Offset { seconds: 9_000, .. }));
    }
}
