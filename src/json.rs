//! Strict JSON and the existing Python-compatible canonical digest contract.
use anyhow::{Result, bail, ensure};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

pub fn load_json(path: impl AsRef<Path>) -> Result<Value> {
    parse_json(&fs::read_to_string(path)?)
}

pub fn parse_json(text: &str) -> Result<Value> {
    let mut parser = Parser { text, cursor: 0 };
    let value = parser.value(0)?;
    parser.space();
    ensure!(parser.cursor == text.len(), "trailing JSON data");
    Ok(value)
}

struct Parser<'a> {
    text: &'a str,
    cursor: usize,
}
impl Parser<'_> {
    fn space(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.cursor)
            .is_some_and(|c| b" \r\n\t".contains(c))
        {
            self.cursor += 1;
        }
    }
    fn take(&mut self, byte: u8) -> bool {
        self.space();
        if self.text.as_bytes().get(self.cursor) == Some(&byte) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn string(&mut self) -> Result<String> {
        self.space();
        let start = self.cursor;
        ensure!(
            self.text.as_bytes().get(start) == Some(&b'"'),
            "expected JSON string"
        );
        self.cursor += 1;
        while let Some(&c) = self.text.as_bytes().get(self.cursor) {
            self.cursor += 1;
            if c == b'\\' {
                self.cursor += 1;
            } else if c == b'"' {
                return Ok(serde_json::from_str(&self.text[start..self.cursor])?);
            }
        }
        bail!("unterminated JSON string")
    }
    fn value(&mut self, depth: usize) -> Result<Value> {
        ensure!(depth < 128, "JSON nesting limit exceeded");
        self.space();
        match self.text.as_bytes().get(self.cursor).copied() {
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b'{') => {
                self.cursor += 1;
                let mut map = Map::new();
                if self.take(b'}') {
                    return Ok(Value::Object(map));
                }
                loop {
                    let key = self.string()?;
                    ensure!(!map.contains_key(&key), "duplicate JSON key");
                    ensure!(self.take(b':'), "expected colon");
                    map.insert(key, self.value(depth + 1)?);
                    if self.take(b'}') {
                        break;
                    }
                    ensure!(self.take(b','), "expected comma");
                }
                Ok(Value::Object(map))
            }
            Some(b'[') => {
                self.cursor += 1;
                let mut values = Vec::new();
                if self.take(b']') {
                    return Ok(Value::Array(values));
                }
                loop {
                    values.push(self.value(depth + 1)?);
                    if self.take(b']') {
                        break;
                    }
                    ensure!(self.take(b','), "expected comma");
                }
                Ok(Value::Array(values))
            }
            Some(b'n' | b't' | b'f') => {
                for (token, value) in [
                    ("null", Value::Null),
                    ("true", Value::Bool(true)),
                    ("false", Value::Bool(false)),
                ] {
                    if self.text[self.cursor..].starts_with(token) {
                        self.cursor += token.len();
                        return Ok(value);
                    }
                }
                bail!("invalid JSON literal")
            }
            Some(b'-' | b'0'..=b'9') => {
                let start = self.cursor;
                while self
                    .text
                    .as_bytes()
                    .get(self.cursor)
                    .is_some_and(|c| b"-+0123456789.eE".contains(c))
                {
                    self.cursor += 1;
                }
                let token = &self.text[start..self.cursor];
                let number: Number = serde_json::from_str(token)?;
                if token.contains(['.', 'e', 'E']) {
                    let float: f64 = token.parse()?;
                    ensure!(float.is_finite(), "non-finite JSON number");
                    Ok(Value::Number(
                        Number::from_f64(float).expect("finite float"),
                    ))
                } else if token == "-0" {
                    Ok(Value::from(0))
                } else {
                    Ok(Value::Number(number))
                }
            }
            _ => bail!("invalid JSON token"),
        }
    }
}

pub fn spec_digest(value: &Value) -> String {
    sha256(canonical_json(value).as_bytes())
}
pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(v) => v.to_string(),
        Value::String(v) => serde_json::to_string(v).expect("string serialization"),
        Value::Number(v) => {
            let text = v.to_string();
            if text.contains(['.', 'e', 'E']) {
                python_float(v.as_f64().expect("finite JSON float"))
            } else {
                text
            }
        }
        Value::Array(v) => format!(
            "[{}]",
            v.iter().map(canonical_json).collect::<Vec<_>>().join(",")
        ),
        Value::Object(v) => {
            let mut keys: Vec<_> = v.keys().collect();
            keys.sort();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|key| format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_json(&v[key])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn python_float(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into();
    }
    let mut buffer = ryu::Buffer::new();
    let shortest = buffer.format_finite(value.abs());
    let (mantissa, exponent) = shortest
        .split_once('e')
        .map(|(m, e)| (m, e.parse::<i32>().unwrap()))
        .unwrap_or((shortest, 0));
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let leading = digits.chars().take_while(|c| *c == '0').count();
    digits.drain(..leading);
    let power = exponent + decimal - leading as i32 - 1;
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    let sign = if value < 0.0 { "-" } else { "" };
    if !(-4..16).contains(&power) {
        let fraction = if digits.len() > 1 {
            format!(".{}", &digits[1..])
        } else {
            String::new()
        };
        format!(
            "{sign}{}{fraction}e{}{:02}",
            &digits[..1],
            if power >= 0 { "+" } else { "-" },
            power.unsigned_abs()
        )
    } else {
        let point = power + 1;
        if point <= 0 {
            format!("{sign}0.{}{}", "0".repeat((-point) as usize), digits)
        } else if point as usize >= digits.len() {
            format!(
                "{sign}{digits}{}.0",
                "0".repeat(point as usize - digits.len())
            )
        } else {
            format!(
                "{sign}{}.{}",
                &digits[..point as usize],
                &digits[point as usize..]
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_json() {
        for text in [
            r#"{"x":1,"x":2}"#,
            r#"{"a":{"x":1,"x":2}}"#,
            "NaN",
            "1e999",
            "[1,]",
            "true false",
            "01",
        ] {
            assert!(parse_json(text).is_err(), "{text}");
        }
    }
    #[test]
    fn canonical_matches_python_numeric_style() {
        let v=parse_json(r#"{"z":1e-6,"a":[-0.0,0.0001,1e16,1.25,1e-5,12345678901234567890123456789],"文本":"中文"}"#).unwrap();
        assert_eq!(
            canonical_json(&v),
            r#"{"a":[-0.0,0.0001,1e+16,1.25,1e-05,12345678901234567890123456789],"z":1e-06,"文本":"中文"}"#
        );
    }
}
