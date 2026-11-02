use bytes::Bytes;

/// Parse one command, preserving arbitrary bytes. Quotes may join unquoted
/// fragments; backslash escapes work both inside and outside either quote type.
pub fn tokenize(line: &[u8]) -> Result<Vec<Bytes>, &'static str> {
    let mut args = Vec::new();
    let mut word = Vec::new();
    let mut started = false;
    let mut quote = None;
    let mut index = 0;
    while index < line.len() {
        let byte = line[index];
        if byte == b'\\' {
            started = true;
            index += 1;
            let escape = *line.get(index).ok_or("trailing backslash")?;
            word.push(match escape {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'b' => 8,
                b'a' => 7,
                b'x' => {
                    let hi = *line
                        .get(index + 1)
                        .ok_or("\\x requires two hexadecimal digits")?;
                    let lo = *line
                        .get(index + 2)
                        .ok_or("\\x requires two hexadecimal digits")?;
                    index += 2;
                    hex(hi)
                        .and_then(|hi| hex(lo).map(|lo| hi * 16 + lo))
                        .ok_or("\\x requires two hexadecimal digits")?
                }
                other => other,
            });
        } else if let Some(delimiter) = quote {
            if byte == delimiter {
                quote = None;
            } else {
                word.push(byte);
            }
        } else if byte == b'\'' || byte == b'"' {
            started = true;
            quote = Some(byte);
        } else if byte.is_ascii_whitespace() {
            if started {
                args.push(Bytes::from(std::mem::take(&mut word)));
                started = false;
                if args.len() > 1024 {
                    return Err("a command may have at most 1024 arguments");
                }
            }
        } else {
            started = true;
            word.push(byte);
        }
        index += 1;
    }
    if quote.is_some() {
        return Err("unterminated quoted argument");
    }
    if started {
        args.push(Bytes::from(word));
    }
    if args.len() > 1024 {
        return Err("a command may have at most 1024 arguments");
    }
    Ok(args)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub fn sensitive(args: &[Bytes]) -> bool {
    args.first().is_some_and(|arg| {
        arg.eq_ignore_ascii_case(b"AUTH")
            || arg.eq_ignore_ascii_case(b"HELLO")
            || arg.eq_ignore_ascii_case(b"ACL")
            || arg.eq_ignore_ascii_case(b"CONFIG")
            || arg.eq_ignore_ascii_case(b"MIGRATE")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_empty_fragments_and_binary_escapes() {
        let actual =
            tokenize(br#"SET 'a b' "" pre"mid"'end' "\x00\xFF\n\r\t" escaped\ space"#).unwrap();
        let expected: &[&[u8]] = &[
            b"SET",
            b"a b",
            b"",
            b"premidend",
            b"\0\xff\n\r\t",
            b"escaped space",
        ];
        assert_eq!(
            actual.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn malformed_input_never_returns_partial_commands() {
        for input in [
            b"SET x 'open".as_slice(),
            b"PING \\",
            b"SET x \\x0",
            b"SET x \\xQ1",
        ] {
            assert!(tokenize(input).is_err());
        }
        assert!(tokenize(b" \t\r\n").unwrap().is_empty());
    }

    #[test]
    fn credential_commands_are_not_history_candidates() {
        for input in [
            b"AUTH secret".as_slice(),
            b"hello 3 auth default password",
            b"ACL SETUSER foo >secret",
            b"CONFIG SET requirepass secret",
        ] {
            assert!(sensitive(&tokenize(input).unwrap()));
        }
        assert!(!sensitive(&tokenize(b"GET value").unwrap()));
    }
}
