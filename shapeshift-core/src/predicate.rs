//! The row filter's predicate (`where:`): which source records a run takes.
//!
//! A small subset of SQL's `WHERE`, so a spec reads the way its table will be queried,
//! and so DuckDB can check the very same text:
//!
//! ```text
//! expr     := or
//! or       := and { "or" and }
//! and      := unary { "and" unary }
//! unary    := "not" unary | "(" expr ")" | test
//! test     := operand cmp operand
//!           | operand [ "not" ] "in" "(" literal { "," literal } ")"
//!           | operand "is" [ "not" ] "null"
//! cmp      := "=" | "!=" | "<" | "<=" | ">" | ">="
//! operand  := path | literal
//! path     := a `from:` path (`user.plan`, `tags.0`, an optional leading `$`), whose
//!             segments may be double-quoted to hold other characters (`"a b".c`)
//! literal  := 'string' ('' for a quote) | number | true | false
//! ```
//!
//! **SQL's three-valued logic.** A test is true, false or unknown; a record is kept only
//! when the whole expression is true. A null or missing operand makes a comparison
//! unknown, and so does a comparison between different kinds of value (a number against
//! a string, anything against an array or object), which is also counted as a *type
//! mismatch* so the run can report it: `status = '500'` against a numeric field keeps
//! nothing, and that should not pass in silence.
//!
//! Parsing happens once, when the spec loads; evaluation runs on the [`Record`] trait,
//! so it reads a parser's tape as cheaply as an owned `serde_json::Value`, and allocates
//! nothing unless it is asked to describe a mismatch.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{Result, ShapeError};
use crate::record::{Kind, Record};
use crate::value::{select_compiled, Segment, MAX_PATH_SEGMENTS};

/// The longest `where` text accepted.
pub const MAX_WHERE_BYTES: usize = 64 * 1024;

/// How deeply `not` and parentheses may nest.
const MAX_DEPTH: usize = 64;

/// A parsed `where` expression.
#[derive(Debug, Clone)]
pub struct Predicate {
    text: String,
    root: Node,
}

/// What testing one record found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tested {
    /// The whole expression was true: the record is kept.
    pub keep: bool,
    /// Some comparison it evaluated met values of different kinds.
    pub mismatch: bool,
}

#[derive(Debug, Clone)]
enum Node {
    Or(Vec<Node>),
    And(Vec<Node>),
    Not(Box<Node>),
    Cmp {
        left: Operand,
        op: CmpOp,
        right: Operand,
    },
    In {
        operand: Operand,
        list: Vec<Lit>,
        negated: bool,
    },
    IsNull {
        operand: Operand,
        negated: bool,
    },
}

#[derive(Debug, Clone)]
enum Operand {
    /// The path as written, and compiled.
    Path(String, Vec<Segment>),
    Lit(Lit),
}

#[derive(Debug, Clone, PartialEq)]
enum Lit {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn holds(self, o: Ordering) -> bool {
        match self {
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Gt => o == Ordering::Greater,
            CmpOp::Ge => o != Ordering::Less,
        }
    }

    fn orders(self) -> bool {
        !matches!(self, CmpOp::Eq | CmpOp::Ne)
    }
}

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        })
    }
}

impl fmt::Display for Lit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lit::Str(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Lit::Int(n) => write!(f, "{n}"),
            Lit::Float(x) => write!(f, "{x:?}"),
            Lit::Bool(b) => write!(f, "{b}"),
        }
    }
}

impl fmt::Display for Operand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operand::Path(text, _) => f.write_str(text),
            Operand::Lit(l) => l.fmt(f),
        }
    }
}

/// A truth value of SQL's three-valued logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn of(b: bool) -> Truth {
        if b {
            Truth::True
        } else {
            Truth::False
        }
    }

    fn not(self) -> Truth {
        match self {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
        }
    }
}

/// An operand's value in one record. Strings are borrowed from the record (or the
/// literal), never copied.
#[derive(Debug, Clone, Copy)]
enum Val<'v> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'v str),
    /// An array or an object: never comparable.
    Nested(Kind),
}

impl Val<'_> {
    fn kind_name(&self) -> &'static str {
        match self {
            Val::Null => "null",
            Val::Bool(_) => "true/false value",
            Val::Int(_) | Val::Float(_) => "number",
            Val::Str(_) => "string",
            Val::Nested(Kind::Array) => "array",
            Val::Nested(_) => "object",
        }
    }

    fn as_f64(&self) -> f64 {
        match *self {
            Val::Int(n) => n as f64,
            Val::Float(x) => x,
            _ => f64::NAN,
        }
    }
}

impl Lit {
    fn val(&self) -> Val<'_> {
        match self {
            Lit::Str(s) => Val::Str(s),
            Lit::Int(n) => Val::Int(*n),
            Lit::Float(x) => Val::Float(*x),
            Lit::Bool(b) => Val::Bool(*b),
        }
    }
}

/// What a comparison of `a` and `b` under `op` comes to, and whether the kinds clashed.
fn compare(a: Val<'_>, op: CmpOp, b: Val<'_>) -> (Truth, bool) {
    let ord = match (a, b) {
        (Val::Null, _) | (_, Val::Null) => return (Truth::Unknown, false),
        (Val::Int(x), Val::Int(y)) => x.cmp(&y),
        (Val::Int(_) | Val::Float(_), Val::Int(_) | Val::Float(_)) => {
            match a.as_f64().partial_cmp(&b.as_f64()) {
                Some(o) => o,
                None => return (Truth::Unknown, false),
            }
        }
        (Val::Str(x), Val::Str(y)) => x.cmp(y),
        (Val::Bool(x), Val::Bool(y)) if !op.orders() => x.cmp(&y),
        _ => return (Truth::Unknown, true),
    };
    (Truth::of(op.holds(ord)), false)
}

/// Where a mismatch goes while evaluating: counted always, described only when asked.
struct Seen<'d> {
    mismatch: bool,
    describe: Option<&'d mut Option<String>>,
}

impl Seen<'_> {
    fn mismatch(&mut self, what: impl FnOnce() -> String) {
        self.mismatch = true;
        if let Some(slot) = self.describe.as_deref_mut() {
            if slot.is_none() {
                *slot = Some(what());
            }
        }
    }
}

impl Predicate {
    /// Parse `where` text. A mistake is a spec error naming the character it is at.
    pub fn parse(text: &str) -> Result<Predicate> {
        let text = text.trim();
        if text.is_empty() {
            return Err(ShapeError::Spec("`where` is empty".into()));
        }
        if text.len() > MAX_WHERE_BYTES {
            return Err(ShapeError::Spec(format!(
                "`where` is {} bytes long; at most {MAX_WHERE_BYTES} are accepted",
                text.len()
            )));
        }
        let tokens = lex(text)?;
        let mut p = Parser {
            text,
            tokens,
            at: 0,
            depth: 0,
        };
        let root = p.or()?;
        if let Some(t) = p.peek() {
            return Err(p.error_at(t.at, format!("unexpected {}", t.tok.describe())));
        }
        Ok(Predicate {
            text: text.to_string(),
            root,
        })
    }

    /// The expression as written (trimmed).
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The paths the expression reads, as segments: the fields a spec with this `where`
    /// reads besides its columns'. The whole-record path (`$`) is left out, since testing
    /// it reads no field.
    pub(crate) fn paths(&self) -> Vec<&[Segment]> {
        fn walk<'n>(node: &'n Node, out: &mut Vec<&'n [Segment]>) {
            let mut operand = |o: &'n Operand| {
                if let Operand::Path(_, segs) = o {
                    if !segs.is_empty() {
                        out.push(segs);
                    }
                }
            };
            match node {
                Node::Or(items) | Node::And(items) => items.iter().for_each(|n| walk(n, out)),
                Node::Not(n) => walk(n, out),
                Node::Cmp { left, right, .. } => {
                    operand(left);
                    operand(right);
                }
                Node::In { operand: o, .. } | Node::IsNull { operand: o, .. } => operand(o),
            }
        }
        let mut out = Vec::new();
        walk(&self.root, &mut out);
        out
    }

    /// Test one record.
    pub fn test<'a, R: Record<'a>>(&self, record: R) -> Tested {
        let mut seen = Seen {
            mismatch: false,
            describe: None,
        };
        let keep = eval(&self.root, record, &mut seen) == Truth::True;
        Tested {
            keep,
            mismatch: seen.mismatch,
        }
    }

    /// Say which comparison met values of different kinds in `record`, if one did: the
    /// first one evaluation reaches.
    pub fn explain_mismatch<'a, R: Record<'a>>(&self, record: R) -> Option<String> {
        let mut slot = None;
        let mut seen = Seen {
            mismatch: false,
            describe: Some(&mut slot),
        };
        eval(&self.root, record, &mut seen);
        slot
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

fn value<'a, 'v, R: Record<'a>>(operand: &'v Operand, record: R) -> Val<'v>
where
    'a: 'v,
{
    match operand {
        Operand::Lit(l) => l.val(),
        Operand::Path(_, segs) => match select_compiled(record, segs) {
            None => Val::Null,
            Some(v) => match v.kind() {
                Kind::Null => Val::Null,
                Kind::Bool => v.as_bool().map_or(Val::Null, Val::Bool),
                // `Int` promises an `i64`; should a model ever break that, the number is
                // still a number.
                Kind::Int => v
                    .as_i64()
                    .map(Val::Int)
                    .or_else(|| v.as_f64().map(Val::Float))
                    .unwrap_or(Val::Null),
                Kind::Float => v.as_f64().map_or(Val::Null, Val::Float),
                Kind::Str => v.as_str().map_or(Val::Null, Val::Str),
                k @ (Kind::Array | Kind::Object) => Val::Nested(k),
            },
        },
    }
}

fn eval<'a, R: Record<'a>>(node: &Node, record: R, seen: &mut Seen<'_>) -> Truth {
    match node {
        Node::Or(items) => {
            let mut out = Truth::False;
            for n in items {
                match eval(n, record, seen) {
                    Truth::True => return Truth::True,
                    Truth::Unknown => out = Truth::Unknown,
                    Truth::False => {}
                }
            }
            out
        }
        Node::And(items) => {
            let mut out = Truth::True;
            for n in items {
                match eval(n, record, seen) {
                    Truth::False => return Truth::False,
                    Truth::Unknown => out = Truth::Unknown,
                    Truth::True => {}
                }
            }
            out
        }
        Node::Not(n) => eval(n, record, seen).not(),
        Node::Cmp { left, op, right } => {
            let (a, b) = (value(left, record), value(right, record));
            let (t, clash) = compare(a, *op, b);
            if clash {
                seen.mismatch(|| clash_text(&format!("{left} {op} {right}"), a, b));
            }
            t
        }
        Node::In {
            operand,
            list,
            negated,
        } => {
            let v = value(operand, record);
            let mut out = Truth::False;
            for l in list {
                let (t, clash) = compare(v, CmpOp::Eq, l.val());
                if clash {
                    let not = if *negated { "not " } else { "" };
                    let test = || format!("{operand} {not}in (…{l}…)");
                    seen.mismatch(|| clash_text(&test(), v, l.val()));
                }
                match t {
                    Truth::True => {
                        out = Truth::True;
                        break;
                    }
                    Truth::Unknown => out = Truth::Unknown,
                    Truth::False => {}
                }
            }
            if *negated {
                out.not()
            } else {
                out
            }
        }
        Node::IsNull { operand, negated } => {
            let null = matches!(value(operand, record), Val::Null);
            Truth::of(null != *negated)
        }
    }
}

fn clash_text(test: &str, a: Val<'_>, b: Val<'_>) -> String {
    let an = |kind: &str| {
        let article = if kind.starts_with(['a', 'o']) {
            "an"
        } else {
            "a"
        };
        format!("{article} {kind}")
    };
    let what = if a.kind_name() == b.kind_name() {
        format!("orders two {}s", a.kind_name())
    } else {
        format!("compares {} with {}", an(a.kind_name()), an(b.kind_name()))
    };
    format!("`{test}` {what}, which is never true")
}

// ---------------------------------------------------------------------------
// Lexing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    LParen,
    RParen,
    Comma,
    Op(CmpOp),
    Lit(Lit),
    /// Segments, whether any was quoted, and the path as written.
    Path(Vec<String>, bool, String),
    Kw(Kw),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kw {
    And,
    Or,
    Not,
    In,
    Is,
    Null,
}

impl Tok {
    fn describe(&self) -> String {
        match self {
            Tok::LParen => "`(`".into(),
            Tok::RParen => "`)`".into(),
            Tok::Comma => "`,`".into(),
            Tok::Op(o) => format!("`{o}`"),
            Tok::Lit(l) => format!("`{l}`"),
            Tok::Path(_, _, raw) => format!("`{raw}`"),
            Tok::Kw(k) => format!("`{}`", format!("{k:?}").to_lowercase()),
        }
    }
}

#[derive(Debug, Clone)]
struct Spanned {
    tok: Tok,
    /// Byte offset into the text.
    at: usize,
}

fn spec_error(text: &str, at: usize, msg: impl fmt::Display) -> ShapeError {
    let col = text[..at.min(text.len())].chars().count();
    // One line of context: the text with a caret under the character, when it fits.
    let shown: String = text.chars().take(120).collect();
    let pointer = if col < 120 {
        format!("\n  {shown}\n  {}^", " ".repeat(col))
    } else {
        String::new()
    };
    ShapeError::Spec(format!(
        "`where`: {msg} (at character {}){pointer}",
        col + 1
    ))
}

fn lex(text: &str) -> Result<Vec<Spanned>> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let err = |at: usize, msg: String| spec_error(text, at, msg);
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        let two = text.get(i..i + 2).unwrap_or("");
        let tok = match c {
            b'(' => {
                i += 1;
                Tok::LParen
            }
            b')' => {
                i += 1;
                Tok::RParen
            }
            b',' => {
                i += 1;
                Tok::Comma
            }
            _ if two == "==" => return Err(err(start, "use `=`, not `==`".into())),
            _ if two == "<>" => return Err(err(start, "use `!=`, not `<>`".into())),
            _ if two == "&&" => return Err(err(start, "use `and`, not `&&`".into())),
            _ if two == "||" => return Err(err(start, "use `or`, not `||`".into())),
            _ if two == "!=" => {
                i += 2;
                Tok::Op(CmpOp::Ne)
            }
            _ if two == "<=" => {
                i += 2;
                Tok::Op(CmpOp::Le)
            }
            _ if two == ">=" => {
                i += 2;
                Tok::Op(CmpOp::Ge)
            }
            b'=' => {
                i += 1;
                Tok::Op(CmpOp::Eq)
            }
            b'<' => {
                i += 1;
                Tok::Op(CmpOp::Lt)
            }
            b'>' => {
                i += 1;
                Tok::Op(CmpOp::Gt)
            }
            b'!' => return Err(err(start, "use `not`, not `!`".into())),
            b'\'' => {
                let (s, end) = quoted(text, i, '\'')
                    .ok_or_else(|| err(start, "this string has no closing `'`".into()))?;
                i = end;
                Tok::Lit(Lit::Str(s))
            }
            b'.' if b.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let (lit, end) = number(text, i).map_err(|m| err(start, m))?;
                i = end;
                Tok::Lit(lit)
            }
            b'-' | b'0'..=b'9' => {
                let (lit, end) = number(text, i).map_err(|m| err(start, m))?;
                i = end;
                Tok::Lit(lit)
            }
            _ if c == b'"'
                || c == b'$'
                || text[i..]
                    .chars()
                    .next()
                    .is_some_and(|ch| ch == '_' || ch.is_alphabetic()) =>
            {
                let (tok, end) = path(text, i).map_err(|(at, m)| err(at, m))?;
                i = end;
                tok
            }
            _ => {
                let ch = text[i..].chars().next().unwrap_or('?');
                return Err(err(start, format!("unexpected `{ch}`")));
            }
        };
        out.push(Spanned { tok, at: start });
    }
    Ok(out)
}

/// A `q`-quoted run starting at `start` (the opening quote), with a doubled quote
/// standing for one: its contents and the offset just past the closing quote.
fn quoted(text: &str, start: usize, q: char) -> Option<(String, usize)> {
    let mut out = String::new();
    let mut chars = text[start + 1..].char_indices().peekable();
    while let Some((off, ch)) = chars.next() {
        if ch == q {
            if chars.peek().map(|&(_, c)| c) == Some(q) {
                chars.next();
                out.push(q);
                continue;
            }
            return Some((out, start + 1 + off + ch.len_utf8()));
        }
        out.push(ch);
    }
    None
}

fn number(text: &str, start: usize) -> std::result::Result<(Lit, usize), String> {
    let b = text.as_bytes();
    let mut i = start;
    if b[i] == b'-' {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let from = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > from
    };
    let whole = digits(&mut i);
    let mut float = false;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        float = true;
        if !digits(&mut i) && !whole {
            return Err("a number needs digits".into());
        }
    } else if !whole {
        return Err("`-` must start a number (there is no arithmetic)".into());
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        float = true;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        if !digits(&mut i) {
            return Err("an exponent needs digits".into());
        }
    }
    if i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.') {
        return Err(
            "a number runs into other characters; a field name starts with a letter \
                    or `_`, or is double-quoted"
                .into(),
        );
    }
    let s = &text[start..i];
    let lit = match (float, s.parse::<i64>()) {
        (false, Ok(n)) => Lit::Int(n),
        _ => Lit::Float(
            s.parse::<f64>()
                .map_err(|_| format!("`{s}` is not a number"))?,
        ),
    };
    Ok((lit, i))
}

/// A path (or a keyword, which is a lone bare segment): `$`, `a.b.0`, `"a b".c`.
fn path(text: &str, start: usize) -> std::result::Result<(Tok, usize), (usize, String)> {
    let b = text.as_bytes();
    let mut i = start;
    let mut segs = Vec::new();
    let mut quoted_any = false;
    let mut rooted = false;
    if b[i] == b'$' {
        rooted = true;
        i += 1;
        if i < b.len() && b[i] == b'.' {
            i += 1;
        }
    }
    // A bare name: letters and digits of any script, `_` and `-` (not first).
    let bare_len = |from: usize| -> usize {
        text[from..]
            .char_indices()
            .find(|&(off, ch)| !(ch.is_alphanumeric() || ch == '_' || (ch == '-' && off > 0)))
            .map_or(text.len() - from, |(off, _)| off)
    };
    loop {
        if i < b.len() && b[i] == b'"' {
            let (s, end) =
                quoted(text, i, '"').ok_or((i, "this name has no closing `\"`".to_string()))?;
            if s.is_empty() {
                return Err((i, "a quoted name cannot be empty".into()));
            }
            segs.push(s);
            quoted_any = true;
            i = end;
        } else if bare_len(i) > 0 {
            let from = i;
            i += bare_len(i);
            segs.push(text[from..i].to_string());
        } else if segs.is_empty() && rooted {
            // `$` alone: the whole record.
            break;
        } else {
            return Err((i, "expected a field name here".into()));
        }
        if segs.len() > MAX_PATH_SEGMENTS {
            return Err((
                start,
                format!("this path has more than {MAX_PATH_SEGMENTS} segments"),
            ));
        }
        if i < b.len() && b[i] == b'.' {
            i += 1;
            continue;
        }
        break;
    }
    let raw = text[start..i].to_string();
    if !rooted && !quoted_any && segs.len() == 1 {
        let kw = match segs[0].to_ascii_lowercase().as_str() {
            "and" => Some(Tok::Kw(Kw::And)),
            "or" => Some(Tok::Kw(Kw::Or)),
            "not" => Some(Tok::Kw(Kw::Not)),
            "in" => Some(Tok::Kw(Kw::In)),
            "is" => Some(Tok::Kw(Kw::Is)),
            "null" => Some(Tok::Kw(Kw::Null)),
            "true" => Some(Tok::Lit(Lit::Bool(true))),
            "false" => Some(Tok::Lit(Lit::Bool(false))),
            _ => None,
        };
        if let Some(t) = kw {
            return Ok((t, i));
        }
    }
    Ok((Tok::Path(segs, quoted_any, raw), i))
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

struct Parser<'t> {
    text: &'t str,
    tokens: Vec<Spanned>,
    at: usize,
    depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Spanned> {
        self.tokens.get(self.at)
    }

    fn next(&mut self) -> Option<Spanned> {
        let t = self.tokens.get(self.at).cloned();
        self.at += 1;
        t
    }

    fn end(&self) -> usize {
        self.text.len()
    }

    fn error_at(&self, at: usize, msg: impl fmt::Display) -> ShapeError {
        spec_error(self.text, at, msg)
    }

    fn eat_kw(&mut self, kw: Kw) -> bool {
        if matches!(self.peek(), Some(Spanned { tok: Tok::Kw(k), .. }) if *k == kw) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn or(&mut self) -> Result<Node> {
        let mut items = vec![self.and()?];
        while self.eat_kw(Kw::Or) {
            items.push(self.and()?);
        }
        Ok(if items.len() == 1 {
            items.pop().unwrap()
        } else {
            Node::Or(items)
        })
    }

    fn and(&mut self) -> Result<Node> {
        let mut items = vec![self.unary()?];
        while self.eat_kw(Kw::And) {
            items.push(self.unary()?);
        }
        Ok(if items.len() == 1 {
            items.pop().unwrap()
        } else {
            Node::And(items)
        })
    }

    fn unary(&mut self) -> Result<Node> {
        let at = self.peek().map_or(self.end(), |t| t.at);
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(self.error_at(at, format!("nests deeper than {MAX_DEPTH} levels")));
        }
        let node = if self.eat_kw(Kw::Not) {
            Node::Not(Box::new(self.unary()?))
        } else if matches!(
            self.peek(),
            Some(Spanned {
                tok: Tok::LParen,
                ..
            })
        ) {
            self.at += 1;
            let inner = self.or()?;
            match self.next() {
                Some(Spanned {
                    tok: Tok::RParen, ..
                }) => inner,
                Some(t) => {
                    return Err(
                        self.error_at(t.at, format!("expected `)`, found {}", t.tok.describe()))
                    )
                }
                None => return Err(self.error_at(at, "this `(` is never closed")),
            }
        } else {
            self.test()?
        };
        self.depth -= 1;
        Ok(node)
    }

    fn operand(&mut self) -> Result<(Operand, usize)> {
        match self.next() {
            Some(Spanned {
                tok: Tok::Path(segs, _, raw),
                at,
            }) => {
                let compiled = segs.into_iter().map(Segment::new).collect();
                Ok((Operand::Path(raw, compiled), at))
            }
            Some(Spanned {
                tok: Tok::Lit(l),
                at,
            }) => Ok((Operand::Lit(l), at)),
            Some(Spanned {
                tok: Tok::Kw(Kw::Null),
                at,
            }) => Err(self.error_at(
                at,
                "compare with null using `is null` or `is not null` (in SQL, `= null` is never \
                 true)",
            )),
            Some(t) => Err(self.error_at(
                t.at,
                format!("expected a field or a value, found {}", t.tok.describe()),
            )),
            None => Err(self.error_at(self.end(), "expected a field or a value at the end")),
        }
    }

    fn test(&mut self) -> Result<Node> {
        let (left, left_at) = self.operand()?;
        let Some(t) = self.next() else {
            return Err(self.error_at(
                left_at,
                format!(
                    "`{left}` alone is not a test: compare it (`{left} = true`) or test it \
                     (`{left} is not null`)"
                ),
            ));
        };
        match t.tok {
            Tok::Op(op) => {
                let (right, right_at) = self.operand()?;
                if let (Operand::Lit(_), Operand::Lit(_)) = (&left, &right) {
                    return Err(self.error_at(
                        left_at,
                        format!(
                            "`{left} {op} {right}` compares two values; name a field on one side \
                             (a field is written bare, or in double quotes; 'single quotes' make \
                             a string)"
                        ),
                    ));
                }
                let bool_lit = |o: &Operand| matches!(o, Operand::Lit(Lit::Bool(_)));
                if op.orders() && (bool_lit(&left) || bool_lit(&right)) {
                    let at = if bool_lit(&left) { left_at } else { right_at };
                    let msg = format!("`{op}` does not order true and false; use `=` or `!=`");
                    return Err(self.error_at(at, msg));
                }
                Ok(Node::Cmp { left, op, right })
            }
            Tok::Kw(Kw::Is) => {
                let negated = self.eat_kw(Kw::Not);
                if self.eat_kw(Kw::Null) {
                    Ok(Node::IsNull {
                        operand: left,
                        negated,
                    })
                } else {
                    let at = self.peek().map_or(self.end(), |t| t.at);
                    Err(self.error_at(at, "`is` takes `null` or `not null`"))
                }
            }
            Tok::Kw(Kw::Not) | Tok::Kw(Kw::In) => {
                let negated = t.tok == Tok::Kw(Kw::Not);
                if negated && !self.eat_kw(Kw::In) {
                    return Err(self.error_at(t.at, format!("expected `not in` after `{left}`")));
                }
                self.in_list(left, negated)
            }
            other => Err(self.error_at(
                t.at,
                format!(
                    "expected a comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`), `in` or `is` after \
                     `{left}`, found {}",
                    other.describe()
                ),
            )),
        }
    }

    fn in_list(&mut self, operand: Operand, negated: bool) -> Result<Node> {
        match self.next() {
            Some(Spanned {
                tok: Tok::LParen, ..
            }) => {}
            Some(t) => {
                return Err(self.error_at(t.at, "`in` takes a list in parentheses: `in ('a', 'b')`"))
            }
            None => {
                return Err(self.error_at(
                    self.end(),
                    "`in` takes a list in parentheses: `in ('a', 'b')`",
                ))
            }
        }
        let mut list = Vec::new();
        loop {
            match self.next() {
                Some(Spanned {
                    tok: Tok::Lit(l), ..
                }) => list.push(l),
                Some(Spanned {
                    tok: Tok::Kw(Kw::Null),
                    at,
                }) => {
                    return Err(self.error_at(
                        at,
                        "an `in` list cannot hold null; test for it with `is null`",
                    ))
                }
                Some(t) => {
                    return Err(self.error_at(
                        t.at,
                        format!(
                            "an `in` list holds values ('text', numbers, true, false), found {}",
                            t.tok.describe()
                        ),
                    ))
                }
                None => return Err(self.error_at(self.end(), "this `in` list is never closed")),
            }
            match self.next() {
                Some(Spanned {
                    tok: Tok::Comma, ..
                }) => continue,
                Some(Spanned {
                    tok: Tok::RParen, ..
                }) => break,
                Some(t) => {
                    return Err(self.error_at(
                        t.at,
                        format!("expected `,` or `)`, found {}", t.tok.describe()),
                    ))
                }
                None => return Err(self.error_at(self.end(), "this `in` list is never closed")),
            }
        }
        Ok(Node::In {
            operand,
            list,
            negated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn keeps(expr: &str, record: &Value) -> bool {
        Predicate::parse(expr).unwrap().test(record).keep
    }

    fn error(expr: &str) -> String {
        Predicate::parse(expr).unwrap_err().to_string()
    }

    #[test]
    fn comparisons_follow_sql() {
        let r = json!({"kind": "gate", "status": 500, "score": 2.5, "ok": true,
                       "user": {"plan": "pro"}, "tags": ["a", "b"], "none": null});
        for (expr, want) in [
            ("kind = 'gate'", true),
            ("kind != 'gate'", false),
            ("status >= 500", true),
            ("status > 500", false),
            ("status < 500.5", true),
            ("status = 500.0", true),
            ("score <= 2.5", true),
            ("ok = true", true),
            ("ok != false", true),
            ("user.plan = 'pro'", true),
            ("$.user.plan = 'pro'", true),
            ("tags.1 = 'b'", true),
            ("'gate' = kind", true),
            ("kind in ('queue', 'gate')", true),
            ("kind not in ('queue', 'gate')", false),
            ("status in (200, 500)", true),
            ("kind < 'h'", true),
            ("kind > 'gat'", true),
        ] {
            assert_eq!(keeps(expr, &r), want, "{expr}");
        }
    }

    #[test]
    fn null_missing_and_mismatched_comparisons_are_unknown() {
        let r = json!({"status": 500, "none": null, "nested": {"a": 1}, "list": [1]});
        let p = |e: &str| Predicate::parse(e).unwrap().test(&r);
        // Unknown is not kept, and `not unknown` is unknown too.
        for e in [
            "missing = 1",
            "missing != 1",
            "none = 1",
            "not (missing = 1)",
            "missing in (1, 2)",
            "missing not in (1, 2)",
        ] {
            assert_eq!(
                p(e),
                Tested {
                    keep: false,
                    mismatch: false
                },
                "{e}"
            );
        }
        // `is null` is how a missing field is tested; missing and null are the same.
        for (e, want) in [
            ("missing is null", true),
            ("none is null", true),
            ("status is null", false),
            ("status is not null", true),
            ("nested is not null", true),
            ("missing is not null", false),
        ] {
            assert_eq!(p(e).keep, want, "{e}");
        }
        // Different kinds, or anything nested: unknown, and counted as a mismatch.
        for e in [
            "status = '500'",
            "status != '500'",
            "nested = 1",
            "list = 1",
            "status in ('500')",
            "nested = nested",
        ] {
            assert_eq!(
                p(e),
                Tested {
                    keep: false,
                    mismatch: true
                },
                "{e}"
            );
        }
        // The rest of the expression still decides.
        assert_eq!(
            p("status = '500' or status = 500"),
            Tested {
                keep: true,
                mismatch: true
            }
        );
        assert_eq!(
            p("status = '500' and status = 501"),
            Tested {
                keep: false,
                mismatch: true
            }
        );
        assert_eq!(
            Predicate::parse("status = '500'")
                .unwrap()
                .explain_mismatch(&r)
                .unwrap(),
            "`status = '500'` compares a number with a string, which is never true"
        );
        assert_eq!(
            Predicate::parse("nested = 1")
                .unwrap()
                .explain_mismatch(&r)
                .unwrap(),
            "`nested = 1` compares an object with a number, which is never true"
        );
        assert_eq!(
            Predicate::parse("list != list")
                .unwrap()
                .explain_mismatch(&r)
                .unwrap(),
            "`list != list` orders two arrays, which is never true"
        );
        assert_eq!(
            Predicate::parse("status = 1").unwrap().explain_mismatch(&r),
            None
        );
    }

    #[test]
    fn three_valued_logic_truth_tables() {
        // t, f and u stand for a true, false and unknown test.
        let r = json!({"one": 1});
        let t = "one = 1";
        let f = "one = 2";
        let u = "missing = 1";
        for (expr, want) in [
            (format!("{t} and {t}"), true),
            (format!("{t} and {u}"), false),
            (format!("{f} and {u}"), false),
            (format!("{t} or {u}"), true),
            (format!("{f} or {u}"), false),
            (format!("not ({f} or {u})"), false),
            (format!("not ({f} and {u})"), true),
            (format!("not ({t} and {u})"), false),
            (format!("not {f}"), true),
            (format!("not not {t}"), true),
        ] {
            assert_eq!(keeps(&expr, &r), want, "{expr}");
        }
    }

    #[test]
    fn precedence_is_sqls() {
        let r = json!({"a": 1, "b": 0, "c": 1});
        // `and` binds tighter than `or`; `not` tighter than `and`.
        assert!(keeps("a = 1 or b = 1 and c = 0", &r));
        assert!(!keeps("(a = 1 or b = 1) and c = 0", &r));
        assert!(keeps("not b = 1 and c = 1", &r));
        assert!(!keeps("not (b = 0 and c = 1)", &r));
        // Keywords are case-insensitive; paths and strings are not.
        assert!(keeps("A IS NULL AND a = 1 OR NOT c = 1", &r));
    }

    #[test]
    fn names_with_other_characters_are_double_quoted() {
        let r = json!({"a b": {"c.d": 1}, "in": "x", "duration-ms": 5, "it's": "y"});
        assert!(keeps(r#""a b"."c.d" = 1"#, &r));
        assert!(keeps(r#""in" = 'x'"#, &r));
        assert!(keeps("duration-ms = 5", &r));
        assert!(keeps(r#""it's" = 'y'"#, &r));
        assert!(keeps("\"it's\" = 'y' and 'it''s' != \"it's\"", &r));
        assert!(keeps(r#"$ is not null"#, &r));
    }

    #[test]
    fn mistakes_are_refused_with_what_to_write_instead() {
        for (expr, says) in [
            ("kind == 'gate'", "use `=`, not `==`"),
            ("kind <> 'gate'", "use `!=`, not `<>`"),
            ("a = 1 && b = 2", "use `and`"),
            ("a = 1 || b = 2", "use `or`"),
            ("!a", "use `not`"),
            ("kind = null", "`is null`"),
            ("kind != null", "`is null`"),
            ("kind in ('a', null)", "cannot hold null"),
            ("kind in 'a'", "list in parentheses"),
            ("kind in (other)", "holds values"),
            ("'gate' = 'kind'", "compares two values"),
            ("ok < true", "does not order true and false"),
            ("active", "alone is not a test"),
            ("kind = 'gate", "no closing `'`"),
            ("\"kind = 1", "no closing `\"`"),
            ("(a = 1", "never closed"),
            ("a = 1)", "unexpected `)`"),
            ("a = 1 b = 2", "unexpected `b`"),
            ("a is 1", "`is` takes `null`"),
            ("a not 1", "`not in`"),
            ("a = 5abc", "runs into other characters"),
            ("a = - 1", "`-` must start a number"),
            ("a. = 1", "expected a field name"),
            ("a = 1 and", "at the end"),
            ("", "empty"),
            ("a = #", "unexpected `#`"),
        ] {
            let e = error(expr);
            assert!(e.contains(says), "{expr:?}: {e}");
        }
        // The position is the character's, with the text and a caret under it.
        let e = error("kind == 'gate'");
        assert!(
            e.contains("(at character 6)\n  kind == 'gate'\n       ^"),
            "{e}"
        );
        let e = error("é = 1 and x == 2");
        assert!(e.contains("(at character 13)"), "{e}");
    }

    #[test]
    fn size_and_depth_are_bounded() {
        let deep = format!("{}a = 1{}", "(".repeat(65), ")".repeat(65));
        assert!(
            error(&deep).contains("nests deeper than 64"),
            "{}",
            error(&deep)
        );
        let fine = format!("{}a = 1{}", "(".repeat(63), ")".repeat(63));
        assert!(Predicate::parse(&fine).is_ok());
        let nots = format!("{}a = 1", "not ".repeat(70));
        assert!(error(&nots).contains("nests deeper"));
        let long = format!("a in ({})", vec!["1"; 40_000].join(","));
        assert!(error(&long).contains("at most 65536"));
    }

    #[test]
    fn numbers_and_strings_read_as_sql_writes_them() {
        let r = json!({"n": -3, "x": 1.5e3, "s": "it's"});
        assert!(keeps("n = -3", &r));
        assert!(keeps("x = 1500", &r));
        assert!(keeps("x = 1.5e3 and x = 15E2 and x > .5", &r));
        assert!(keeps("s = 'it''s'", &r));
        assert!(keeps("n < 9223372036854775807", &r));
        assert!(keeps("n < 99999999999999999999", &r)); // past i64: a float
                                                        // A value past i64 (a u64) is a number, present and comparable.
        let big = json!({"umax": 18446744073709551615u64});
        assert!(keeps("umax is not null and umax > 1", &big));
        assert!(keeps("umax >= 18446744073709551615", &big));
        assert_eq!(Predicate::parse("  s = 'x'  ").unwrap().text(), "s = 'x'");
    }
}
