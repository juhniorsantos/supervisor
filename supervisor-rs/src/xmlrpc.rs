//! A small, dependency-free XML-RPC implementation: just enough of the
//! spec to parse the `methodCall`s that `supervisorctl` sends and to render
//! the `methodResponse`/`fault` documents it expects.
//!
//! Supported value types: `int`/`i4`, `boolean`, `string`, `double`,
//! `base64` (decoded to a string), `array`, `struct`, and `nil` (treated as
//! an empty string). Untyped `<value>text</value>` is treated as a string,
//! per the spec.

/// An XML-RPC value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Str(String),
    Double(f64),
    Array(Vec<Value>),
    Struct(Vec<(String, Value)>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            Value::Int(i) => Some(*i != 0),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tokeniser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// `<name>` (open), `</name>` (close) or `<name/>` (self-closing).
    Open(String),
    Close(String),
    SelfClose(String),
    Text(String),
}

fn tokenize(input: &str) -> Vec<Tok> {
    let mut toks = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            // Skip XML declarations / processing instructions / comments.
            if input[i..].starts_with("<?") {
                if let Some(end) = input[i..].find("?>") {
                    i += end + 2;
                    continue;
                }
            }
            if input[i..].starts_with("<!--") {
                if let Some(end) = input[i..].find("-->") {
                    i += end + 3;
                    continue;
                }
            }
            let end = match input[i..].find('>') {
                Some(e) => i + e,
                None => break,
            };
            let inner = input[i + 1..end].trim();
            if let Some(name) = inner.strip_prefix('/') {
                toks.push(Tok::Close(name.trim().to_string()));
            } else if let Some(name) = inner.strip_suffix('/') {
                toks.push(Tok::SelfClose(name.trim().to_string()));
            } else {
                // Drop any attributes: keep the element name only.
                let name = inner.split_whitespace().next().unwrap_or("").to_string();
                toks.push(Tok::Open(name));
            }
            i = end + 1;
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b'<' {
                i += 1;
            }
            let text = unescape(&input[start..i]);
            toks.push(Tok::Text(text));
        }
    }
    toks
}

fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn new(toks: Vec<Tok>) -> Self {
        Parser { toks, pos: 0 }
    }

    /// Peek the next token, skipping whitespace-only text.
    fn peek(&mut self) -> Option<&Tok> {
        while let Some(Tok::Text(t)) = self.toks.get(self.pos) {
            if t.trim().is_empty() {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let _ = self.peek();
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect_open(&mut self, name: &str) -> Result<(), String> {
        match self.next() {
            Some(Tok::Open(n)) if n == name => Ok(()),
            other => Err(format!("expected <{name}>, got {other:?}")),
        }
    }

    fn expect_close(&mut self, name: &str) -> Result<(), String> {
        match self.next() {
            Some(Tok::Close(n)) if n == name => Ok(()),
            other => Err(format!("expected </{name}>, got {other:?}")),
        }
    }

    /// Read the raw text content until the next tag.
    fn read_text(&mut self) -> String {
        if let Some(Tok::Text(t)) = self.toks.get(self.pos) {
            let t = t.clone();
            self.pos += 1;
            t
        } else {
            String::new()
        }
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        self.expect_open("value")?;
        let value = match self.peek().cloned() {
            Some(Tok::Open(t)) => match t.as_str() {
                "int" | "i4" | "i8" => {
                    self.next();
                    let n = self.read_text();
                    self.expect_close(&t)?;
                    Value::Int(n.trim().parse().map_err(|_| "bad int")?)
                }
                "boolean" => {
                    self.next();
                    let n = self.read_text();
                    self.expect_close("boolean")?;
                    Value::Bool(n.trim() == "1")
                }
                "double" => {
                    self.next();
                    let n = self.read_text();
                    self.expect_close("double")?;
                    Value::Double(n.trim().parse().map_err(|_| "bad double")?)
                }
                "string" => {
                    self.next();
                    let s = self.read_text();
                    self.expect_close("string")?;
                    Value::Str(s)
                }
                "base64" => {
                    self.next();
                    let s = self.read_text();
                    self.expect_close("base64")?;
                    Value::Str(s) // surfaced as opaque text
                }
                "array" => self.parse_array()?,
                "struct" => self.parse_struct()?,
                other => return Err(format!("unsupported value type <{other}>")),
            },
            Some(Tok::SelfClose(t)) if t == "nil" => {
                self.next();
                Value::Str(String::new())
            }
            // Untyped value: raw text is a string.
            _ => Value::Str(self.read_text()),
        };
        self.expect_close("value")?;
        Ok(value)
    }

    fn parse_array(&mut self) -> Result<Value, String> {
        self.expect_open("array")?;
        self.expect_open("data")?;
        let mut items = Vec::new();
        while let Some(Tok::Open(n)) = self.peek() {
            if n == "value" {
                items.push(self.parse_value()?);
            } else {
                break;
            }
        }
        self.expect_close("data")?;
        self.expect_close("array")?;
        Ok(Value::Array(items))
    }

    fn parse_struct(&mut self) -> Result<Value, String> {
        self.expect_open("struct")?;
        let mut members = Vec::new();
        while let Some(Tok::Open(n)) = self.peek() {
            if n != "member" {
                break;
            }
            self.expect_open("member")?;
            self.expect_open("name")?;
            let name = self.read_text();
            self.expect_close("name")?;
            let value = self.parse_value()?;
            self.expect_close("member")?;
            members.push((name, value));
        }
        self.expect_close("struct")?;
        Ok(Value::Struct(members))
    }
}

/// Parse an XML-RPC `methodCall` into `(method_name, params)`.
pub fn parse_method_call(body: &str) -> Result<(String, Vec<Value>), String> {
    let mut p = Parser::new(tokenize(body));
    p.expect_open("methodCall")?;
    p.expect_open("methodName")?;
    let method = p.read_text().trim().to_string();
    p.expect_close("methodName")?;

    let mut params = Vec::new();
    if let Some(Tok::Open(n)) = p.peek() {
        if n == "params" {
            p.expect_open("params")?;
            while let Some(Tok::Open(n)) = p.peek() {
                if n != "param" {
                    break;
                }
                p.expect_open("param")?;
                params.push(p.parse_value()?);
                p.expect_close("param")?;
            }
            p.expect_close("params")?;
        }
    }
    Ok((method, params))
}

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

fn write_value(out: &mut String, v: &Value) {
    out.push_str("<value>");
    match v {
        Value::Int(i) => {
            out.push_str("<int>");
            out.push_str(&i.to_string());
            out.push_str("</int>");
        }
        Value::Bool(b) => {
            out.push_str("<boolean>");
            out.push(if *b { '1' } else { '0' });
            out.push_str("</boolean>");
        }
        Value::Double(d) => {
            out.push_str("<double>");
            out.push_str(&d.to_string());
            out.push_str("</double>");
        }
        Value::Str(s) => {
            out.push_str("<string>");
            out.push_str(&escape(s));
            out.push_str("</string>");
        }
        Value::Array(items) => {
            out.push_str("<array><data>");
            for it in items {
                write_value(out, it);
            }
            out.push_str("</data></array>");
        }
        Value::Struct(members) => {
            out.push_str("<struct>");
            for (name, val) in members {
                out.push_str("<member><name>");
                out.push_str(&escape(name));
                out.push_str("</name>");
                write_value(out, val);
                out.push_str("</member>");
            }
            out.push_str("</struct>");
        }
    }
    out.push_str("</value>");
}

/// Render a successful `methodResponse` carrying a single return value.
pub fn serialize_response(value: &Value) -> String {
    let mut out = String::from("<?xml version=\"1.0\"?>\n<methodResponse><params><param>");
    write_value(&mut out, value);
    out.push_str("</param></params></methodResponse>\n");
    out
}

/// Render a `methodCall` document for the client side.
pub fn serialize_method_call(method: &str, params: &[Value]) -> String {
    let mut out = String::from("<?xml version=\"1.0\"?>\n<methodCall><methodName>");
    out.push_str(&escape(method));
    out.push_str("</methodName><params>");
    for p in params {
        out.push_str("<param>");
        write_value(&mut out, p);
        out.push_str("</param>");
    }
    out.push_str("</params></methodCall>\n");
    out
}

/// Parse a `methodResponse`, returning the value on success or `(code, msg)`
/// for a fault.
pub fn parse_method_response(xml: &str) -> Result<Value, (i32, String)> {
    let mut p = Parser::new(tokenize(xml));
    p.expect_open("methodResponse")
        .map_err(|e| (-1, e))?;
    match p.peek().cloned() {
        Some(Tok::Open(n)) if n == "fault" => {
            p.expect_open("fault").map_err(|e| (-1, e))?;
            let v = p.parse_value().map_err(|e| (-1, e))?;
            if let Value::Struct(members) = v {
                let code = members
                    .iter()
                    .find(|(k, _)| k == "faultCode")
                    .and_then(|(_, v)| if let Value::Int(i) = v { Some(*i as i32) } else { None })
                    .unwrap_or(-1);
                let msg = members
                    .iter()
                    .find(|(k, _)| k == "faultString")
                    .and_then(|(_, v)| v.as_str())
                    .unwrap_or("")
                    .to_string();
                Err((code, msg))
            } else {
                Err((-1, "malformed fault".into()))
            }
        }
        Some(Tok::Open(n)) if n == "params" => {
            p.expect_open("params").map_err(|e| (-1, e))?;
            p.expect_open("param").map_err(|e| (-1, e))?;
            let v = p.parse_value().map_err(|e| (-1, e))?;
            Ok(v)
        }
        other => Err((-1, format!("unexpected response content: {other:?}"))),
    }
}

/// Render a `fault` response with the given code and message.
pub fn serialize_fault(code: i32, message: &str) -> String {
    let fault = Value::Struct(vec![
        ("faultCode".to_string(), Value::Int(code as i64)),
        ("faultString".to_string(), Value::Str(message.to_string())),
    ]);
    let mut out = String::from("<?xml version=\"1.0\"?>\n<methodResponse><fault>");
    write_value(&mut out, &fault);
    out.push_str("</fault></methodResponse>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_method_call_with_params() {
        let xml = "<?xml version='1.0'?><methodCall><methodName>supervisor.startProcess</methodName>\
            <params><param><value><string>web</string></value></param>\
            <param><value><boolean>1</boolean></value></param></params></methodCall>";
        let (method, params) = parse_method_call(xml).unwrap();
        assert_eq!(method, "supervisor.startProcess");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], Value::Str("web".into()));
        assert_eq!(params[1], Value::Bool(true));
    }

    #[test]
    fn parses_no_params() {
        let xml = "<methodCall><methodName>supervisor.getAllProcessInfo</methodName><params></params></methodCall>";
        let (method, params) = parse_method_call(xml).unwrap();
        assert_eq!(method, "supervisor.getAllProcessInfo");
        assert!(params.is_empty());
    }

    #[test]
    fn untyped_value_is_a_string() {
        let xml = "<methodCall><methodName>m</methodName><params><param><value>hi</value></param></params></methodCall>";
        let (_, params) = parse_method_call(xml).unwrap();
        assert_eq!(params[0], Value::Str("hi".into()));
    }

    #[test]
    fn round_trips_a_struct() {
        let v = Value::Struct(vec![
            ("name".into(), Value::Str("web".into())),
            ("pid".into(), Value::Int(42)),
        ]);
        let xml = serialize_response(&v);
        assert!(xml.contains("<name>name</name><value><string>web</string></value>"));
        assert!(xml.contains("<name>pid</name><value><int>42</int></value>"));
    }

    #[test]
    fn escapes_special_characters() {
        let v = Value::Str("a < b & c".into());
        let mut out = String::new();
        write_value(&mut out, &v);
        assert!(out.contains("a &lt; b &amp; c"));
    }
}
