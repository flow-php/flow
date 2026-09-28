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

/// Epoch microseconds of `YYYY-MM-DDTHH:MM:SS[.f]` (1–6 fraction digits) + `Z` or `±HH:MM`, every field in range - the
/// instant `DateTimeType::cast` yields, computed without timelib. `None` for every other string, which timelib parses.
pub fn iso_instant_micros(bytes: &[u8]) -> Option<i64> {
    let byte_at = |at: usize| bytes.get(at).copied();
    let number = |at: usize, count: usize| -> Option<i64> {
        bytes
            .get(at..at + count)?
            .iter()
            .try_fold(0i64, |value, digit| digit.is_ascii_digit().then(|| value * 10 + i64::from(digit - b'0')))
    };

    if [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')]
        .iter()
        .any(|(at, separator)| byte_at(*at) != Some(*separator))
    {
        return None;
    }

    let (year, month, day) = (number(0, 4)?, number(5, 2)?, number(8, 2)?);
    let (hour, minute, second) = (number(11, 2)?, number(14, 2)?, number(17, 2)?);

    if year > 9999 || !checkdate(month, day, year) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let mut at = 19;
    let mut fraction = 0;

    if byte_at(at) == Some(b'.') {
        let digits = bytes[at + 1..].iter().take_while(|byte| byte.is_ascii_digit()).count();

        if !(1..=6).contains(&digits) {
            return None;
        }

        fraction = number(at + 1, digits)? * 10i64.pow(6 - digits as u32);
        at += 1 + digits;
    }

    let offset = match byte_at(at) {
        Some(b'Z') => {
            at += 1;

            0
        }
        Some(sign @ (b'+' | b'-')) if byte_at(at + 3) == Some(b':') => {
            let (hours, minutes) = (number(at + 1, 2)?, number(at + 4, 2)?);

            if hours > 23 || minutes > 59 {
                return None;
            }

            at += 6;

            (hours * 3600 + minutes * 60) * if sign == b'-' { -1 } else { 1 }
        }
        _ => return None,
    };

    if at != bytes.len() {
        return None;
    }

    let seconds = days_from_civil(year, month, day)? * 86_400 + hour * 3600 + minute * 60 + second - offset;

    Some(seconds * 1_000_000 + fraction)
}

/// `DaysFromCivil::of()`: days since 1970-01-01 of a proleptic Gregorian date, `None` on i64 overflow.
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let year = year - i64::from(month <= 2);
    let era = (if year >= 0 { year } else { year.checked_sub(399)? }) / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era.checked_mul(146_097)?.checked_add(day_of_era - 719_468)
}
