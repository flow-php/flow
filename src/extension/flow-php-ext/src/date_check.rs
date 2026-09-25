/// The zone suffix `DateTimeType::ISO_DATE_TIME` matched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsoSuffix {
    Naive,
    Zulu,
    Offset,
}

/// `DateTimeType::ISO_DATE_TIME` + `checkdate()`, with the zone suffix it matched.
pub fn iso_date_time_gate(bytes: &[u8]) -> Option<IsoSuffix> {
    // PCRE `$` without the D modifier also matches before one final "\n"
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let byte_at = |at: usize| bytes.get(at).copied();
    let number_at = |at: usize, max: u32| {
        bytes
            .get(at..at + 2)
            .filter(|run| run.iter().all(u8::is_ascii_digit))
            .is_some_and(|run| u32::from(run[0] - b'0') * 10 + u32::from(run[1] - b'0') <= max)
    };

    if !(iso_date_prefix(bytes)
        && matches!(byte_at(10), Some(b'T' | b' '))
        && number_at(11, 24)
        && byte_at(13) == Some(b':')
        && number_at(14, 59))
    {
        return None;
    }

    let mut at = 16;

    if byte_at(at) == Some(b':') && number_at(at + 1, 60) {
        at += 3;

        if byte_at(at) == Some(b'.') {
            let fraction = bytes[at + 1..].iter().take_while(|byte| byte.is_ascii_digit()).count();

            if !(1..=9).contains(&fraction) {
                return None;
            }

            at += 1 + fraction;
        }
    }

    let suffix = match byte_at(at) {
        Some(b'Z') => {
            at += 1;

            IsoSuffix::Zulu
        }
        Some(b'+' | b'-') if number_at(at + 1, 99) => {
            at += 3;

            // a bare ±hh takes any two digits; the hour of ±hhmm and ±hh:mm stops at 24
            let colon = usize::from(byte_at(at) == Some(b':'));

            if number_at(at + colon, 59) && number_at(at - 2, 24) {
                at += colon + 2;
            }

            IsoSuffix::Offset
        }
        _ => IsoSuffix::Naive,
    };

    (at == bytes.len()).then_some(suffix)
}

/// `DateType::ISO_DATE` + `checkdate()`.
pub fn iso_date_gate(bytes: &[u8]) -> bool {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);

    bytes.len() == 10 && iso_date_prefix(bytes)
}

fn iso_date_prefix(bytes: &[u8]) -> bool {
    let digits_at = |at: usize, count: usize| {
        bytes
            .get(at..at + count)
            .is_some_and(|run| run.iter().all(u8::is_ascii_digit))
    };

    if !(digits_at(0, 4)
        && bytes.get(4) == Some(&b'-')
        && digits_at(5, 2)
        && bytes.get(7) == Some(&b'-')
        && digits_at(8, 2))
    {
        return false;
    }

    let number = |range: std::ops::Range<usize>| {
        bytes[range]
            .iter()
            .fold(0i64, |value, digit| value * 10 + i64::from(digit - b'0'))
    };

    checkdate(number(5..7), number(8..10), number(0..4))
}

pub fn checkdate(month: i64, day: i64, year: i64) -> bool {
    if !(1..=32767).contains(&year) || !(1..=12).contains(&month) || day < 1 {
        return false;
    }

    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };

    day <= days
}
