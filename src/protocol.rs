use bytes::{Buf, BufMut, Bytes, BytesMut};
use redis_protocol::resp2::{
    decode::decode_bytes_mut,
    encode::encode_bytes as resp2_encode_bytes,
    types::{BytesFrame as Resp2Frame, Resp2Frame as Resp2FrameTrait},
};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RespVersion {
    Resp2,
    Resp3,
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidFrame(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::InvalidFrame(msg) => write!(f, "ERR Protocol error: {msg}"),
        }
    }
}

/// Unified response representation that serializes cleanly to both RESP2 and RESP3.
#[derive(Clone, Debug, PartialEq)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Null,
    Double(f64),
    Array(Vec<RespValue>),
    /// Ordered key-value map: serialized as flat array in RESP2, `%N\r\n` map in RESP3.
    Map(Vec<(RespValue, RespValue)>),
    /// Sorted-set member/score pairs: serialized as flat `[member, score_str, ...]` in RESP2,
    /// and `[[member, Double(score)], ...]` in RESP3.
    ScoredArray(Vec<(Bytes, f64)>),
}

impl RespValue {
    pub fn ok() -> Self {
        Self::SimpleString(Bytes::from_static(b"OK"))
    }

    pub fn pong() -> Self {
        Self::SimpleString(Bytes::from_static(b"PONG"))
    }

    pub fn queued() -> Self {
        Self::SimpleString(Bytes::from_static(b"QUEUED"))
    }

    pub fn simple(s: impl Into<Bytes>) -> Self {
        Self::SimpleString(s.into())
    }

    pub fn bulk(b: impl Into<Bytes>) -> Self {
        Self::BulkString(b.into())
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    pub fn wrong_arity(cmd: &str) -> Self {
        Self::Error(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd.to_lowercase()
        ))
    }

    pub fn syntax_err() -> Self {
        Self::Error("ERR syntax error".to_string())
    }

    pub fn not_integer_err() -> Self {
        Self::Error("ERR value is not an integer or out of range".to_string())
    }

    pub fn not_float_err() -> Self {
        Self::Error("ERR value is not a valid float".to_string())
    }

    pub fn to_resp2_string(&self) -> String {
        let mut buf = BytesMut::new();
        encode_resp(self, RespVersion::Resp2, &mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }
}

/// Format a Redis double value matching Redis conventions (`inf`, `-inf`, clean decimal).
pub fn format_score(score: f64) -> String {
    if score.is_infinite() {
        if score.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if score == 0.0 {
        "0".to_string()
    } else if score.fract() == 0.0 && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        let s = format!("{score:.17}");
        let trimmed = s.trim_end_matches('0').trim_end_matches('.');
        trimmed.to_string()
    }
}

/// Zero-copy decode of a single Redis command from `buf`.
///
/// Supports both standard RESP multibulk arrays (`*N\r\n$L\r\n...\r\n`) using
/// `redis_protocol::resp2::decode::decode_bytes_mut` AND inline text commands
/// (e.g., `PING\r\n` over `nc` / `telnet`).
pub fn decode_command(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    // Skip stray empty CRLF/LF lines between commands
    while !buf.is_empty() {
        if buf[0] == b'\r' {
            if buf.len() >= 2 && buf[1] == b'\n' {
                buf.advance(2);
                continue;
            } else if buf.len() == 1 {
                return Ok(None);
            } else {
                buf.advance(1);
                continue;
            }
        } else if buf[0] == b'\n' {
            buf.advance(1);
            continue;
        }
        break;
    }

    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] == b'*' {
        match decode_bytes_mut(buf) {
            Ok(Some((frame, _consumed, _raw))) => {
                let args = frame_to_args(frame)?;
                if args.is_empty() {
                    // Empty array (*0\r\n): recursively decode next frame if any
                    return decode_command(buf);
                }
                Ok(Some(args))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ProtocolError::InvalidFrame(e.to_string())),
        }
    } else {
        decode_inline_command(buf)
    }
}

fn frame_to_args(frame: Resp2Frame) -> Result<Vec<Bytes>, ProtocolError> {
    match frame {
        Resp2Frame::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => out.push(b),
                    Resp2Frame::Integer(n) => out.push(Bytes::from(n.to_string())),
                    Resp2Frame::Null => out.push(Bytes::new()),
                    _ => {
                        return Err(ProtocolError::InvalidFrame(
                            "expected bulk string in command array".to_string(),
                        ))
                    }
                }
            }
            Ok(out)
        }
        Resp2Frame::Null => Ok(Vec::new()),
        _ => Err(ProtocolError::InvalidFrame(
            "expected multibulk array".to_string(),
        )),
    }
}

fn decode_inline_command(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, ProtocolError> {
    let Some(nl_pos) = buf.iter().position(|&b| b == b'\n') else {
        return Ok(None);
    };

    let line_raw = buf.split_to(nl_pos + 1);
    let mut line = &line_raw[..nl_pos];
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }

    let args = split_inline_args(line)?;
    if args.is_empty() {
        return decode_command(buf);
    }
    Ok(Some(args))
}

fn split_inline_args(line: &[u8]) -> Result<Vec<Bytes>, ProtocolError> {
    let mut args = Vec::new();
    let mut i = 0;
    let len = line.len();

    while i < len {
        while i < len && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        if i >= len {
            break;
        }

        if line[i] == b'"' || line[i] == b'\'' {
            let quote = line[i];
            i += 1;
            let mut current = Vec::new();
            let mut closed = false;
            while i < len {
                if line[i] == b'\\' && quote == b'"' && i + 1 < len {
                    i += 1;
                    match line[i] {
                        b'n' => current.push(b'\n'),
                        b'r' => current.push(b'\r'),
                        b't' => current.push(b'\t'),
                        b'"' => current.push(b'"'),
                        b'\\' => current.push(b'\\'),
                        other => current.push(other),
                    }
                } else if line[i] == quote {
                    i += 1;
                    closed = true;
                    break;
                } else {
                    current.push(line[i]);
                }
                i += 1;
            }
            if !closed {
                return Err(ProtocolError::InvalidFrame(
                    "unbalanced quotes in request".to_string(),
                ));
            }
            args.push(Bytes::from(current));
        } else {
            let start = i;
            while i < len && line[i] != b' ' && line[i] != b'\t' {
                i += 1;
            }
            args.push(Bytes::copy_from_slice(&line[start..i]));
        }
    }

    Ok(args)
}

fn to_resp2_frame(value: &RespValue) -> Resp2Frame {
    match value {
        RespValue::SimpleString(s) => Resp2Frame::SimpleString(s.clone()),
        RespValue::Error(e) => Resp2Frame::Error(e.clone().into()),
        RespValue::Integer(n) => Resp2Frame::Integer(*n),
        RespValue::BulkString(b) => Resp2Frame::BulkString(b.clone()),
        RespValue::Null => Resp2Frame::Null,
        RespValue::Double(d) => Resp2Frame::BulkString(Bytes::from(format_score(*d))),
        RespValue::Array(items) => {
            Resp2Frame::Array(items.iter().map(to_resp2_frame).collect())
        }
        RespValue::Map(pairs) => {
            let mut flat = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                flat.push(to_resp2_frame(k));
                flat.push(to_resp2_frame(v));
            }
            Resp2Frame::Array(flat)
        }
        RespValue::ScoredArray(pairs) => {
            let mut flat = Vec::with_capacity(pairs.len() * 2);
            for (member, score) in pairs {
                flat.push(Resp2Frame::BulkString(member.clone()));
                flat.push(Resp2Frame::BulkString(Bytes::from(format_score(*score))));
            }
            Resp2Frame::Array(flat)
        }
    }
}

/// Encode a `RespValue` into `out` according to the connection's negotiated RESP version.
pub fn encode_resp(value: &RespValue, version: RespVersion, out: &mut BytesMut) {
    match version {
        RespVersion::Resp2 => {
            let frame = to_resp2_frame(value);
            let needed = frame.encode_len(false);
            let start = out.len();
            out.resize(start + needed, 0);
            match resp2_encode_bytes(&mut out[start..], &frame, false) {
                Ok(written) => out.truncate(start + written),
                Err(_) => {
                    out.truncate(start);
                    out.extend_from_slice(b"-ERR internal protocol encoding failure\r\n");
                }
            }
        }
        RespVersion::Resp3 => encode_resp3(value, out),
    }
}

fn encode_resp3(value: &RespValue, out: &mut BytesMut) {
    match value {
        RespValue::SimpleString(s) => {
            out.put_u8(b'+');
            out.extend_from_slice(s);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            out.put_u8(b'-');
            out.extend_from_slice(e.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            out.put_u8(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(b) => {
            out.put_u8(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Null => {
            out.extend_from_slice(b"_\r\n");
        }
        RespValue::Double(d) => {
            out.put_u8(b',');
            out.extend_from_slice(format_score(*d).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Array(items) => {
            out.put_u8(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_resp3(item, out);
            }
        }
        RespValue::Map(pairs) => {
            out.put_u8(b'%');
            out.extend_from_slice(pairs.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_resp3(k, out);
                encode_resp3(v, out);
            }
        }
        RespValue::ScoredArray(pairs) => {
            out.put_u8(b'*');
            out.extend_from_slice(pairs.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for (member, score) in pairs {
                out.extend_from_slice(b"*2\r\n");
                encode_resp3(&RespValue::BulkString(member.clone()), out);
                encode_resp3(&RespValue::Double(*score), out);
            }
        }
    }
}
