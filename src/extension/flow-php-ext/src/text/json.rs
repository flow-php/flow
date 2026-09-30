//! `php_json_escape_string()` (ext/json/json_encoder.c) for the flags the native JSON writer accepts.

use crate::text::Error;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    pub unescaped_slashes: bool,
    pub unescaped_unicode: bool,
    pub preserve_zero_fraction: bool,
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn unit(out: &mut Vec<u8>, unit: u32) {
    out.extend_from_slice(&[
        b'\\',
        b'u',
        HEX[(unit >> 12 & 0xf) as usize],
        HEX[(unit >> 8 & 0xf) as usize],
        HEX[(unit >> 4 & 0xf) as usize],
        HEX[(unit & 0xf) as usize],
    ]);
}

/// `text` as a JSON string; nothing is appended when it is not UTF-8.
pub fn string(out: &mut Vec<u8>, text: &[u8], flags: &Flags) -> Result<(), Error> {
    if text.is_ascii() {
        out.push(b'"');

        let mut rest = text;

        // runs of bytes written as they are, then the one that is escaped
        while let Some(at) = rest.iter().position(|byte| matches!(byte, 0..=0x1f | b'"' | b'\\' | b'/')) {
            out.extend_from_slice(&rest[..at]);
            ascii(out, rest[at], flags);
            rest = &rest[at + 1..];
        }

        out.extend_from_slice(rest);
        out.push(b'"');

        return Ok(());
    }

    let text = std::str::from_utf8(text).map_err(|_| Error::Utf8)?;

    out.push(b'"');

    for character in text.chars() {
        let code = character as u32;

        if code < 0x80 {
            ascii(out, code as u8, flags);
        } else if flags.unescaped_unicode && !(0x2028..=0x2029).contains(&code) {
            out.extend_from_slice(character.encode_utf8(&mut [0; 4]).as_bytes());
        } else if code >= 0x1_0000 {
            let code = code - 0x1_0000;

            unit(out, code >> 10 | 0xd800);
            unit(out, code & 0x3ff | 0xdc00);
        } else {
            unit(out, code);
        }
    }

    out.push(b'"');

    Ok(())
}

fn ascii(out: &mut Vec<u8>, byte: u8, flags: &Flags) {
    match byte {
        b'"' => out.extend_from_slice(b"\\\""),
        b'\\' => out.extend_from_slice(b"\\\\"),
        b'/' if !flags.unescaped_slashes => out.extend_from_slice(b"\\/"),
        0x08 => out.extend_from_slice(b"\\b"),
        0x0c => out.extend_from_slice(b"\\f"),
        b'\n' => out.extend_from_slice(b"\\n"),
        b'\r' => out.extend_from_slice(b"\\r"),
        b'\t' => out.extend_from_slice(b"\\t"),
        0..=0x1f => unit(out, u32::from(byte)),
        _ => out.push(byte),
    }
}

#[cfg(test)]
mod tests {
    use super::{string, Flags};
    use crate::text::Error;

    fn escaped(text: &[u8], flags: &Flags) -> Result<String, Error> {
        let mut out = Vec::new();

        string(&mut out, text, flags).map(|()| String::from_utf8(out).unwrap())
    }

    #[test]
    fn default_flags_escape_slashes_controls_and_non_ascii() {
        let flags = Flags::default();

        assert_eq!(escaped(b"", &flags).unwrap(), r#""""#);
        assert_eq!(escaped(b"a\"b\\c/d", &flags).unwrap(), r#""a\"b\\c\/d""#);
        assert_eq!(escaped(b"\x08\x0c\n\r\t\x00\x1f\x7f", &flags).unwrap(), "\"\\b\\f\\n\\r\\t\\u0000\\u001f\x7f\"");
        assert_eq!(escaped("<>&'".as_bytes(), &flags).unwrap(), r#""<>&'""#);
        assert_eq!(escaped("zażółć".as_bytes(), &flags).unwrap(), r#""za\u017c\u00f3\u0142\u0107""#);
    }

    #[test]
    fn a_character_beyond_the_basic_plane_is_a_surrogate_pair() {
        assert_eq!(escaped("😀".as_bytes(), &Flags::default()).unwrap(), r#""\ud83d\ude00""#);
        assert_eq!(escaped("\u{10000}\u{10ffff}".as_bytes(), &Flags::default()).unwrap(), r#""\ud800\udc00\udbff\udfff""#);
    }

    #[test]
    fn unescaped_slashes_and_unicode() {
        let slashes = Flags { unescaped_slashes: true, ..Flags::default() };
        let unicode = Flags { unescaped_unicode: true, ..Flags::default() };

        assert_eq!(escaped("a/ż".as_bytes(), &slashes).unwrap(), r#""a/\u017c""#);
        assert_eq!(escaped("a/ż😀".as_bytes(), &unicode).unwrap(), "\"a\\/ż😀\"");
        // the line terminators stay escaped without JSON_UNESCAPED_LINE_TERMINATORS
        assert_eq!(escaped("\u{2027}\u{2028}\u{2029}\u{202a}".as_bytes(), &unicode).unwrap(), "\"\u{2027}\\u2028\\u2029\u{202a}\"");
    }

    #[test]
    fn invalid_utf8_is_refused_and_appends_nothing() {
        for text in [&b"\xff"[..], b"a\xc3\x28", b"\xed\xa0\x80", b"\xc0\xaf", b"\xf4\x90\x80\x80", b"abc\xe2\x82"] {
            let mut out = b"kept".to_vec();

            assert_eq!(string(&mut out, text, &Flags::default()), Err(Error::Utf8));
            assert_eq!(out, b"kept");
        }
    }
}
