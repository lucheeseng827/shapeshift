//! A pulled source's position, and the rules every incremental source follows.
//!
//! One definition, shared by every source that pulls (HTTP now, MySQL next): a run never
//! skips a record, never lands one twice, and always makes progress. The rules are the
//! seven of `docs/SPEC.md` §2.6:
//!
//! 1. A position is `at v` (every record below `v` was read, and the group at `v` as it
//!    stood), or `inside v, after k` (with a tiebreak: also every record at `v` with a key up
//!    to `k`).
//! 2. From `at v` a run reads values `>= v - lookback`: those up to `v` are *replay*, those
//!    above are *new*. From `inside v, after k`, the records at `v` past `k` and those above
//!    are new, and with a lookback those from `v - lookback` up to (not at) `v` are replay.
//! 3. Replay must not double: an incremental source needs a merge key, or a `strict` field
//!    (unique and only growing), whose position's own value is skipped. (Checked by the spec.)
//! 4. Bounds count new records only; replay is never charged.
//! 5. A bound needs a declared ascending order. With a tiebreak a run may stop after any
//!    record; without one only at the end of a group, and a group larger than `max_tie`
//!    fails the run. A run that reaches the end commits `at` the last value.
//! 6. A bounded run with anything new commits a later position.
//! 7. A late record below the replay cutoff (`v - lookback`) is missed: the stated limit.
//!
//! [`Tracker`] is the rules as code. A source asks it [`Tracker::lower_bound`] for what to
//! request, hands it each record's cursor value (and key), and does what the
//! [`Verdict`] says; at the end, [`Tracker::commit`] is the position to write in the snapshot.

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::error::{Result, ShapeError};

/// A cursor value, as it compares: a number, an instant (an RFC 3339 string), or text.
#[derive(Debug, Clone, PartialEq)]
pub enum Cursor {
    /// An integer, compared exactly (an id past 2^53 keeps its order).
    Int(i128),
    Num(f64),
    Time(i64),
    Text(String),
}

impl Cursor {
    /// Read a cursor value from a record's field. Null or a nested value is not one.
    pub fn of(v: &Value) -> Option<Cursor> {
        match v {
            Value::Number(n) => match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => Some(Cursor::Int(i as i128)),
                (_, Some(u)) => Some(Cursor::Int(u as i128)),
                _ => n.as_f64().map(Cursor::Num),
            },
            Value::String(s) => Some(match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(t) => Cursor::Time(t.timestamp_micros()),
                Err(_) => Cursor::Text(s.clone()),
            }),
            _ => None,
        }
    }

    /// As a float: a number, or a float that is not finite as a source writes it
    /// (`"Infinity"`, `"-Infinity"`, `"NaN"`).
    fn float(&self) -> Option<f64> {
        match self {
            Cursor::Num(f) => Some(*f),
            Cursor::Int(i) => Some(*i as f64),
            Cursor::Text(s) => match s.as_str() {
                "Infinity" => Some(f64::INFINITY),
                "-Infinity" => Some(f64::NEG_INFINITY),
                "NaN" => Some(f64::NAN),
                _ => None,
            },
            Cursor::Time(_) => None,
        }
    }

    fn cmp(&self, other: &Cursor) -> Result<Ordering> {
        // Floats in the databases' order: -Infinity, the finite values, Infinity, then NaN
        // (equal to itself).
        let floats = |a: f64, b: f64| match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => a.partial_cmp(&b).expect("neither is NaN"),
        };
        Ok(match (self, other) {
            (Cursor::Int(a), Cursor::Int(b)) => a.cmp(b),
            (Cursor::Time(a), Cursor::Time(b)) => a.cmp(b),
            // Text is compared as text unless both are floats that are not finite.
            (Cursor::Text(a), Cursor::Text(b))
                if !(self.float().is_some() && other.float().is_some()) =>
            {
                a.as_bytes().cmp(b.as_bytes())
            }
            (a, b) if a.float().is_some() && b.float().is_some() => {
                floats(a.float().expect("some"), b.float().expect("some"))
            }
            (a, b) => {
                return Err(ShapeError::Source(format!(
                    "the cursor's values are of different kinds ({} and {}): they cannot be \
                     ordered",
                    a.kind(),
                    b.kind()
                )))
            }
        })
    }

    fn kind(&self) -> &'static str {
        match self {
            Cursor::Int(_) | Cursor::Num(_) => "a number",
            Cursor::Time(_) => "a timestamp",
            Cursor::Text(_) => "text",
        }
    }
}

/// A committed position: `at v`, or `inside v, after k`. Values are kept as the source
/// gave them, so the next run asks the source in its own terms.
#[derive(Debug, Clone, PartialEq)]
pub enum Position {
    At(Value),
    Inside(Value, Value),
}

impl Position {
    /// The JSON a snapshot records: `{"at": v}` or `{"inside": v, "after": k}`.
    pub fn to_json(&self) -> Value {
        match self {
            Position::At(v) => json!({ "at": v }),
            Position::Inside(v, k) => json!({ "inside": v, "after": k }),
        }
    }

    /// Read a recorded position.
    pub fn from_json(v: &Value) -> Result<Position> {
        let bad = || {
            ShapeError::Source(format!(
                "the table records a source position this build cannot read: {v}"
            ))
        };
        if let Some(at) = v.get("at") {
            Cursor::of(at).ok_or_else(bad)?;
            return Ok(Position::At(at.clone()));
        }
        match (v.get("inside"), v.get("after")) {
            (Some(at), Some(k)) => {
                Cursor::of(at).ok_or_else(bad)?;
                Cursor::of(k).ok_or_else(bad)?;
                Ok(Position::Inside(at.clone(), k.clone()))
            }
            _ => Err(bad()),
        }
    }

    /// The value the position is at.
    pub fn value(&self) -> &Value {
        match self {
            Position::At(v) | Position::Inside(v, _) => v,
        }
    }
}

/// How a source orders and tells its records apart.
#[derive(Debug, Clone, Default)]
pub struct Rules {
    /// The cursor is unique and only grows (an auto-increment id): a record at the position's
    /// value was read already, so there is no replay.
    pub strict: bool,
    /// Records come in ascending cursor order (and, with a tiebreak, ascending key within a
    /// value). Bounds need it.
    pub ordered: bool,
    /// A unique field that orders records within one cursor value.
    pub tiebreak: bool,
    /// How far below the position the replay reaches, in milliseconds (a timestamp cursor).
    pub lookback_ms: Option<i64>,
    /// The largest group of records sharing one value a bounded run without a tiebreak may
    /// read to its end.
    pub max_tie: u64,
}

/// The run's limits. Each counts from the first new record; replay is never charged.
#[derive(Debug, Clone, Default)]
pub struct Bounds {
    pub max_records: Option<u64>,
    pub max_pages: Option<u64>,
    pub max_wait: Option<Duration>,
}

impl Bounds {
    pub fn any(&self) -> bool {
        self.max_records.is_some() || self.max_pages.is_some() || self.max_wait.is_some()
    }
}

/// What to do with a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Land it. `replay`: it is at the position's value (or within the lookback), read again.
    Take { replay: bool },
    /// Already read (below the replay cutoff, or at or before the position's key).
    Skip,
    /// The run stops here: it does not land this record, and reads no more.
    Stop,
}

/// What a run did with the source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub new: u64,
    pub replayed: u64,
    pub skipped: u64,
    /// The run stopped at a bound before the source's end.
    pub bounded: bool,
}

/// The position rules, applied to one run.
#[derive(Debug)]
pub struct Tracker {
    rules: Rules,
    bounds: Bounds,
    start: Option<Position>,
    start_value: Option<Cursor>,
    start_key: Option<Cursor>,
    cutoff: Option<Cursor>,
    /// The last record taken, its value and key, as given and as compared.
    last: Option<(Value, Option<Value>, Cursor)>,
    /// The largest value taken (an unordered source commits it).
    max: Option<(Value, Cursor)>,
    /// The group being read: its value and how many records of it were taken.
    group: Option<(Cursor, u64)>,
    first_new: Option<Instant>,
    pages_since_new: u64,
    /// A bound was reached; without a tiebreak, the group at this value is read to its end.
    limit_at: Option<Cursor>,
    stopped: bool,
    progress: Progress,
}

impl Tracker {
    /// A tracker starting from `start` (the table's recorded position; `None` on the first
    /// run). Bounds without an ascending order are refused (rule 5).
    pub fn new(rules: Rules, bounds: Bounds, start: Option<Position>) -> Result<Tracker> {
        if bounds.any() && !rules.ordered {
            return Err(ShapeError::Source(
                "a bounded run (--max-records, --max-pages, --max-wait) needs the source's \
                 order: declare `incremental.order: asc`. Without it the run reads to the \
                 end"
                .into(),
            ));
        }
        let (start_value, start_key) = match &start {
            None => (None, None),
            Some(Position::At(v)) => (Cursor::of(v), None),
            Some(Position::Inside(v, k)) => (Cursor::of(v), Cursor::of(k)),
        };
        let cutoff = match (&start, &start_value, rules.lookback_ms) {
            (Some(_), Some(v), Some(l)) if !rules.strict => Some(match v {
                Cursor::Time(t) => Cursor::Time(t - l.saturating_mul(1000)),
                other => {
                    return Err(ShapeError::Source(format!(
                        "`incremental.lookback` is a duration, and the cursor is {}: it applies \
                         to a timestamp cursor only",
                        other.kind()
                    )))
                }
            }),
            (Some(Position::At(_)), Some(v), _) => Some(v.clone()),
            _ => None,
        };
        Ok(Tracker {
            rules,
            bounds,
            start,
            start_value,
            start_key,
            cutoff,
            last: None,
            max: None,
            group: None,
            first_new: None,
            pages_since_new: 0,
            limit_at: None,
            stopped: false,
            progress: Progress::default(),
        })
    }

    /// What to ask the source for: the lowest value to read, and whether everything at that
    /// value was read already (a strict cursor), so a source that can say "greater than" may.
    /// The tracker skips those records either way. `None`: read from the beginning.
    pub fn lower_bound(&self) -> Option<(Value, bool)> {
        match &self.start {
            None => None,
            Some(Position::At(v)) if self.rules.strict => Some((v.clone(), true)),
            // From `inside v`, the lookback (if any) reaches below `v` as from `at v`.
            Some(Position::Inside(v, _)) | Some(Position::At(v)) => {
                match (&self.cutoff, Cursor::of(v)) {
                    (Some(Cursor::Time(t)), Some(Cursor::Time(_))) => {
                        let at = chrono::DateTime::from_timestamp_micros(*t)
                            .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true));
                        Some((at.map(Value::String).unwrap_or_else(|| v.clone()), false))
                    }
                    _ => Some((v.clone(), false)),
                }
            }
        }
    }

    /// Whether the run should fetch another page: not once it has stopped, nor once a bound
    /// is reached with a tiebreak (it may stop anywhere). Without a tiebreak, a run at its
    /// bound reads on to the end of the group it is in.
    pub fn wants_more(&self) -> bool {
        !self.stopped && !(self.limit_at.is_some() && self.rules.tiebreak)
    }

    /// A page ended.
    pub fn page_done(&mut self) -> Result<()> {
        if self.first_new.is_some() {
            self.pages_since_new += 1;
            if self
                .bounds
                .max_pages
                .is_some_and(|m| self.pages_since_new >= m)
            {
                self.reach_limit();
            }
        }
        Ok(())
    }

    fn reach_limit(&mut self) {
        if self.limit_at.is_none() {
            self.limit_at = Some(
                self.last
                    .as_ref()
                    .map(|l| l.2.clone())
                    .unwrap_or(Cursor::Int(i128::MIN)),
            );
        }
    }

    /// Judge one record by its cursor value and (with a tiebreak) its key.
    pub fn record(&mut self, value: &Value, key: Option<&Value>) -> Result<Verdict> {
        if self.stopped {
            return Ok(Verdict::Stop);
        }
        let c = Cursor::of(value).ok_or_else(|| {
            ShapeError::Source(format!(
                "a record has no usable cursor value ({value}): the cursor field must be a \
                 number or a string on every record"
            ))
        })?;
        let k = match (self.rules.tiebreak, key) {
            (true, Some(k)) => Some(Cursor::of(k).ok_or_else(|| {
                ShapeError::Source(format!("a record has no usable tiebreak key ({k})"))
            })?),
            (true, None) => return Err(ShapeError::Source("a record has no tiebreak key".into())),
            (false, _) => None,
        };
        // A lookback is a duration: it needs a timestamp cursor, which the first record shows,
        // so a first run fails here rather than commit a position no later run could use.
        if self.rules.lookback_ms.is_some() && !self.rules.strict && !matches!(c, Cursor::Time(_)) {
            return Err(ShapeError::Source(format!(
                "`incremental.lookback` is a duration, and the cursor value {value} is {}: it \
                 applies to a timestamp cursor (RFC 3339) only",
                c.kind()
            )));
        }
        // A declared order is checked, not trusted.
        if self.rules.ordered {
            if let Some((_, lk, lc)) = &self.last {
                let o = c.cmp(lc)?;
                let back = o == Ordering::Less
                    || (o == Ordering::Equal
                        && match (&k, lk.as_ref().and_then(Cursor::of)) {
                            (Some(k), Some(lk)) => k.cmp(&lk)? != Ordering::Greater,
                            _ => false,
                        });
                if back && self.rules.tiebreak {
                    return Err(ShapeError::Source(format!(
                        "the source returned {value} after a later record: it is not in the \
                         ascending order `incremental.order: asc` declares"
                    )));
                }
                if o == Ordering::Less {
                    return Err(ShapeError::Source(format!(
                        "the source returned {value} after a later value: it is not in the \
                         ascending order `incremental.order: asc` declares"
                    )));
                }
            }
        }
        // Already read, replay, or new (rule 2).
        let replay = match (&self.start, &self.start_value) {
            (None, _) | (_, None) => false,
            (Some(Position::At(_)), Some(v)) => {
                let o = c.cmp(v)?;
                if self.rules.strict {
                    if o != Ordering::Greater {
                        self.progress.skipped += 1;
                        return Ok(Verdict::Skip);
                    }
                    false
                } else {
                    if let Some(cut) = &self.cutoff {
                        if c.cmp(cut)? == Ordering::Less {
                            self.progress.skipped += 1;
                            return Ok(Verdict::Skip);
                        }
                    }
                    o != Ordering::Greater
                }
            }
            (Some(Position::Inside(_, _)), Some(v)) => match c.cmp(v)? {
                // Below `v`: replay within the lookback, which a late record may have reached;
                // read already below it.
                Ordering::Less => {
                    let within = match &self.cutoff {
                        Some(cut) => c.cmp(cut)? != Ordering::Less,
                        None => false,
                    };
                    if !within {
                        self.progress.skipped += 1;
                        return Ok(Verdict::Skip);
                    }
                    true
                }
                Ordering::Equal => {
                    let after = match (&k, &self.start_key) {
                        (Some(k), Some(sk)) => k.cmp(sk)? == Ordering::Greater,
                        _ => true,
                    };
                    if !after {
                        self.progress.skipped += 1;
                        return Ok(Verdict::Skip);
                    }
                    false
                }
                Ordering::Greater => false,
            },
        };
        if replay {
            self.take(value, key, c, true)?;
            return Ok(Verdict::Take { replay: true });
        }
        // New: within the bounds, or (without a tiebreak) finishing the group at the bound.
        if let Some(at) = &self.limit_at {
            let same_group = !self.rules.tiebreak && c.cmp(at)? == Ordering::Equal;
            if !same_group {
                self.stopped = true;
                self.progress.bounded = true;
                return Ok(Verdict::Stop);
            }
        }
        if self.first_new.is_none() {
            self.first_new = Some(Instant::now());
        }
        self.take(value, key, c, false)?;
        self.progress.new += 1;
        let records_done = self
            .bounds
            .max_records
            .is_some_and(|m| self.progress.new >= m);
        let time_done = self
            .bounds
            .max_wait
            .zip(self.first_new)
            .is_some_and(|(w, t)| t.elapsed() >= w);
        if records_done || time_done {
            self.reach_limit();
        }
        Ok(Verdict::Take { replay: false })
    }

    fn take(&mut self, value: &Value, key: Option<&Value>, c: Cursor, replay: bool) -> Result<()> {
        // The group: a run that reached its bound inside one, without a tiebreak, reads it to
        // its end, so a group it finishes that way is capped.
        match &mut self.group {
            Some((g, n)) if g.cmp(&c)? == Ordering::Equal => *n += 1,
            _ => self.group = Some((c.clone(), 1)),
        }
        if self.limit_at.is_some() && !self.rules.tiebreak && !replay {
            if let Some((_, n)) = &self.group {
                if *n > self.rules.max_tie {
                    return Err(ShapeError::Source(format!(
                        "more than {} records share the cursor value {value} \
                         (`incremental.max_tie`): a bounded run without a tiebreak must read a \
                         whole group. Add `incremental.tiebreak` (a unique field), or order by a \
                         finer field",
                        self.rules.max_tie
                    )));
                }
            }
        }
        if self
            .max
            .as_ref()
            .map_or(Ok(true), |(_, m)| c.cmp(m).map(|o| o == Ordering::Greater))?
        {
            self.max = Some((value.clone(), c.clone()));
        }
        self.last = Some((value.clone(), key.cloned(), c));
        if replay {
            self.progress.replayed += 1;
        }
        Ok(())
    }

    /// `--max-wait` measured while no record arrives: once it has passed since the first new
    /// record, the bound is reached (a source that waits on a slow server checks this between
    /// records). Stopping then is safe at any record: without a tiebreak the run commits
    /// `at` the last value, whose group the next run reads again.
    pub fn time_up(&mut self) -> bool {
        let up = self
            .bounds
            .max_wait
            .zip(self.first_new)
            .is_some_and(|(w, t)| t.elapsed() >= w);
        if up {
            self.reach_limit();
        }
        up
    }

    /// The run ends without a [`Verdict::Stop`]: the source ran out, or (`more`) it had more
    /// to give but the run stopped asking at its bound, as one with a tiebreak does at a page's
    /// end. A bound reached with more to give is a bounded run.
    pub fn finish(&mut self, more: bool) {
        if more && self.limit_at.is_some() {
            self.stopped = true;
            self.progress.bounded = true;
        }
    }

    /// The position to commit: where the next run starts. With nothing read, the position
    /// the run started from (rule 6: it says so, and moves nothing).
    pub fn commit(&self) -> Option<Position> {
        let Some((lv, lk, _)) = &self.last else {
            return self.start.clone();
        };
        // Nothing new: replay alone (a lookback reaches below the position) never moves it,
        // least of all back.
        if self.progress.new == 0 && self.start.is_some() {
            return self.start.clone();
        }
        if self.progress.bounded && self.rules.tiebreak {
            return Some(Position::Inside(
                lv.clone(),
                lk.clone().unwrap_or(Value::Null),
            ));
        }
        if self.rules.ordered {
            return Some(Position::At(lv.clone()));
        }
        // Unordered: the largest value read, so the next run asks from there.
        let max = self
            .max
            .as_ref()
            .map(|(v, _)| v.clone())
            .unwrap_or(lv.clone());
        Some(Position::At(max))
    }

    /// What the run did.
    pub fn progress(&self) -> &Progress {
        &self.progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(ordered: bool, tiebreak: bool) -> Rules {
        Rules {
            ordered,
            tiebreak,
            max_tie: 1_000,
            ..Rules::default()
        }
    }

    /// Feed `(value, key)` records; return the verdicts and the commit.
    fn run(t: &mut Tracker, recs: &[(Value, Option<Value>)]) -> Vec<Verdict> {
        let mut out = Vec::new();
        for (v, k) in recs {
            let verdict = t.record(v, k.as_ref()).unwrap();
            out.push(verdict);
            if verdict == Verdict::Stop {
                break;
            }
        }
        out
    }

    fn n(v: i64) -> (Value, Option<Value>) {
        (json!(v), None)
    }

    #[test]
    fn replay_is_never_charged_and_a_group_is_finished_at_a_bound() {
        // At 10: the records at 10 are replay; the bound of 2 counts new ones only.
        let bounds = Bounds {
            max_records: Some(2),
            ..Bounds::default()
        };
        let mut t =
            Tracker::new(rules(true, false), bounds, Some(Position::At(json!(10)))).unwrap();
        let v = run(&mut t, &[n(10), n(10), n(11), n(12), n(12), n(12), n(13)]);
        use Verdict::*;
        assert_eq!(
            v,
            vec![
                Take { replay: true },
                Take { replay: true },
                Take { replay: false },
                Take { replay: false }, // the limit, inside group 12 ...
                Take { replay: false }, // ... read to its end
                Take { replay: false },
                Stop
            ]
        );
        assert_eq!(t.commit(), Some(Position::At(json!(12))));
        assert_eq!(t.progress().replayed, 2);
        assert!(t.progress().bounded);
    }

    #[test]
    fn a_tiebreak_lets_a_run_stop_inside_a_group_and_the_next_resumes_after_it() {
        let bounds = Bounds {
            max_records: Some(2),
            ..Bounds::default()
        };
        let key = |v: i64, k: i64| (json!(v), Some(json!(k)));
        let mut t = Tracker::new(rules(true, true), bounds.clone(), None).unwrap();
        let v = run(&mut t, &[key(5, 1), key(5, 2), key(5, 3)]);
        assert_eq!(v.last(), Some(&Verdict::Stop));
        let at = t.commit().unwrap();
        assert_eq!(at, Position::Inside(json!(5), json!(2)));
        // The next run: records at 5 up to key 2 are skipped, the rest are new.
        let mut t = Tracker::new(rules(true, true), bounds, Some(at)).unwrap();
        let v = run(
            &mut t,
            &[key(5, 1), key(5, 2), key(5, 3), key(6, 1), key(7, 1)],
        );
        assert_eq!(&v[..2], &[Verdict::Skip, Verdict::Skip]);
        assert_eq!(t.progress().new, 2);
        assert_eq!(t.commit(), Some(Position::Inside(json!(6), json!(1))));
    }

    #[test]
    fn resuming_inside_a_group_replays_the_lookback_below_it() {
        let mut r = rules(true, true);
        r.lookback_ms = Some(5 * 60_000);
        let at = Position::Inside(json!("2026-11-01T10:00:00Z"), json!(5));
        let mut t = Tracker::new(r, Bounds::default(), Some(at)).unwrap();
        assert_eq!(t.lower_bound().unwrap().0, json!("2026-11-01T09:55:00Z"));
        let rec = |t: &mut Tracker, v: &str, k: i64| t.record(&json!(v), Some(&json!(k))).unwrap();
        assert_eq!(
            rec(&mut t, "2026-11-01T09:50:00Z", 1),
            Verdict::Skip,
            "below the cutoff"
        );
        assert_eq!(
            rec(&mut t, "2026-11-01T09:57:00Z", 9),
            Verdict::Take { replay: true },
            "a late record"
        );
        assert_eq!(
            rec(&mut t, "2026-11-01T10:00:00Z", 3),
            Verdict::Skip,
            "the key covers it"
        );
        assert_eq!(
            rec(&mut t, "2026-11-01T10:00:00Z", 6),
            Verdict::Take { replay: false }
        );
        // Without a lookback nothing below `v` is read.
        let mut t = Tracker::new(
            rules(true, true),
            Bounds::default(),
            Some(Position::Inside(json!(10), json!(5))),
        )
        .unwrap();
        assert_eq!(t.lower_bound().unwrap().0, json!(10));
        assert_eq!(t.record(&json!(9), Some(&json!(1))).unwrap(), Verdict::Skip);
    }

    #[test]
    fn a_float_cursor_orders_its_values_that_are_not_finite_as_the_database_does() {
        let order = [
            json!("-Infinity"),
            json!(-1.5),
            json!(1),
            json!(1.0e300),
            json!("Infinity"),
            json!("NaN"),
        ];
        for w in order.windows(2) {
            let (a, b) = (Cursor::of(&w[0]).unwrap(), Cursor::of(&w[1]).unwrap());
            assert_eq!(a.cmp(&b).unwrap(), Ordering::Less, "{} < {}", w[0], w[1]);
        }
        let nan = Cursor::of(&json!("NaN")).unwrap();
        assert_eq!(nan.cmp(&nan).unwrap(), Ordering::Equal);
        // Ordinary text is still text.
        let (a, b) = (
            Cursor::of(&json!("NaN")).unwrap(),
            Cursor::of(&json!("apple")).unwrap(),
        );
        assert_eq!(a.cmp(&b).unwrap(), Ordering::Less);
        // A run reads them in that order and resumes from the last.
        let mut t = Tracker::new(rules(false, false), Bounds::default(), None).unwrap();
        for v in &order {
            t.record(v, None).unwrap();
        }
        assert_eq!(t.commit(), Some(Position::At(json!("NaN"))));
        let mut t = Tracker::new(rules(false, false), Bounds::default(), t.commit()).unwrap();
        assert_eq!(t.record(&json!("Infinity"), None).unwrap(), Verdict::Skip);
    }

    #[test]
    fn replay_alone_never_moves_the_position_back() {
        let mut r = rules(true, true);
        r.lookback_ms = Some(5 * 60_000);
        let start = Position::Inside(json!("2026-11-01T10:00:00Z"), json!(5));
        let mut t = Tracker::new(r.clone(), Bounds::default(), Some(start.clone())).unwrap();
        t.record(&json!("2026-11-01T09:57:00Z"), Some(&json!(9)))
            .unwrap();
        assert_eq!(t.commit(), Some(start));
        // From `at v`, a lookback read with nothing at `v` any more keeps `at v` too.
        let at = Position::At(json!("2026-11-01T10:00:00Z"));
        let mut t = Tracker::new(r, Bounds::default(), Some(at.clone())).unwrap();
        t.record(&json!("2026-11-01T09:58:00Z"), Some(&json!(1)))
            .unwrap();
        assert_eq!(t.commit(), Some(at));
    }

    #[test]
    fn max_wait_ends_a_run_while_nothing_arrives() {
        let bounds = Bounds {
            max_wait: Some(Duration::from_millis(5)),
            ..Bounds::default()
        };
        let mut t = Tracker::new(rules(true, false), bounds, None).unwrap();
        assert!(!t.time_up(), "nothing new yet: the clock has not started");
        t.record(&json!(1), None).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        assert!(t.time_up());
        t.finish(true);
        assert!(t.progress().bounded);
        assert_eq!(t.commit(), Some(Position::At(json!(1))));
    }

    #[test]
    fn a_bound_reached_at_a_page_end_with_a_tiebreak_stops_inside_the_group() {
        let bounds = Bounds {
            max_records: Some(2),
            ..Bounds::default()
        };
        let mut t = Tracker::new(rules(true, true), bounds, None).unwrap();
        t.record(&json!(5), Some(&json!(1))).unwrap();
        t.record(&json!(5), Some(&json!(2))).unwrap();
        t.page_done().unwrap();
        assert!(!t.wants_more());
        t.finish(true);
        assert!(t.progress().bounded);
        assert_eq!(t.commit(), Some(Position::Inside(json!(5), json!(2))));
    }

    #[test]
    fn a_lookback_on_a_number_cursor_fails_the_first_run_and_max_tie_waits_for_the_bound() {
        let mut r = rules(true, false);
        r.lookback_ms = Some(60_000);
        let mut t = Tracker::new(r, Bounds::default(), None).unwrap();
        let e = t.record(&json!(1), None).unwrap_err().to_string();
        assert!(e.contains("timestamp"), "{e}");
        // A group larger than max_tie that the run never has to finish at its bound is fine.
        let mut r = rules(true, false);
        r.max_tie = 2;
        let bounds = Bounds {
            max_records: Some(10),
            ..Bounds::default()
        };
        let mut t = Tracker::new(r, bounds, None).unwrap();
        for _ in 0..5 {
            t.record(&json!(3), None).unwrap();
        }
        t.record(&json!(4), None).unwrap();
    }

    #[test]
    fn a_group_past_max_tie_fails_a_bounded_run_without_a_tiebreak() {
        let mut r = rules(true, false);
        r.max_tie = 3;
        let bounds = Bounds {
            max_records: Some(2),
            ..Bounds::default()
        };
        let mut t = Tracker::new(r, bounds, None).unwrap();
        for _ in 0..3 {
            t.record(&json!(7), None).unwrap();
        }
        let e = t.record(&json!(7), None).unwrap_err().to_string();
        assert!(e.contains("more than 3") && e.contains("tiebreak"), "{e}");
    }

    #[test]
    fn a_strict_cursor_skips_its_own_value_and_a_lookback_widens_the_replay() {
        let mut r = rules(true, false);
        r.strict = true;
        let mut t = Tracker::new(r, Bounds::default(), Some(Position::At(json!(10)))).unwrap();
        assert_eq!(t.lower_bound(), Some((json!(10), true)));
        let v = run(&mut t, &[n(10), n(11)]);
        assert_eq!(v, vec![Verdict::Skip, Verdict::Take { replay: false }]);

        let mut r = rules(true, false);
        r.lookback_ms = Some(60_000);
        let at = json!("2026-01-01T00:10:00Z");
        let mut t = Tracker::new(r, Bounds::default(), Some(Position::At(at))).unwrap();
        assert_eq!(t.lower_bound().unwrap().0, json!("2026-01-01T00:09:00Z"));
        let v = run(
            &mut t,
            &[
                (json!("2026-01-01T00:08:59Z"), None),
                (json!("2026-01-01T00:09:30Z"), None),
                (json!("2026-01-01T00:11:00Z"), None),
            ],
        );
        assert_eq!(
            v,
            vec![
                Verdict::Skip,
                Verdict::Take { replay: true },
                Verdict::Take { replay: false }
            ]
        );
        // A lookback is a duration: a number cursor cannot have one.
        let mut r = rules(true, false);
        r.lookback_ms = Some(1);
        assert!(Tracker::new(r, Bounds::default(), Some(Position::At(json!(3)))).is_err());
    }

    #[test]
    fn bounds_need_an_order_and_an_unordered_run_commits_the_largest_value() {
        let bounds = Bounds {
            max_records: Some(1),
            ..Bounds::default()
        };
        assert!(Tracker::new(rules(false, false), bounds, None).is_err());
        let mut t = Tracker::new(rules(false, false), Bounds::default(), None).unwrap();
        run(&mut t, &[n(5), n(9), n(2)]);
        assert_eq!(t.commit(), Some(Position::At(json!(9))));
        // A declared order is checked.
        let mut t = Tracker::new(rules(true, false), Bounds::default(), None).unwrap();
        t.record(&json!(5), None).unwrap();
        assert!(t.record(&json!(4), None).is_err());
    }

    #[test]
    fn nothing_new_commits_the_same_position_and_positions_round_trip() {
        let at = Position::At(json!(3));
        let t = Tracker::new(rules(true, false), Bounds::default(), Some(at.clone())).unwrap();
        assert_eq!(t.commit(), Some(at.clone()));
        let inside = Position::Inside(json!("2026-01-01T00:00:00Z"), json!(7));
        for p in [at, inside] {
            assert_eq!(Position::from_json(&p.to_json()).unwrap(), p);
        }
        assert!(Position::from_json(&json!({"at": [1]})).is_err());
        // Ids past 2^53 keep their order.
        let big = 9_007_199_254_740_993u64;
        let mut t = Tracker::new(
            rules(true, false),
            Bounds::default(),
            Some(Position::At(json!(big - 1))),
        )
        .unwrap();
        assert_eq!(
            t.record(&json!(big), None).unwrap(),
            Verdict::Take { replay: false }
        );
    }

    #[test]
    fn the_progress_loop_reaches_records_past_a_tie_group_larger_than_the_limit() {
        // 101 records at one value, then later values, pulled with a limit of 100 until a run
        // reads nothing new: the re-read of the 101 must not use up the limit.
        let mut source: Vec<(Value, Option<Value>)> = (0..101).map(|_| n(1)).collect();
        source.extend((2..=5).map(n));
        let bounds = Bounds {
            max_records: Some(100),
            ..Bounds::default()
        };
        let mut position = None;
        let mut landed_new = 0;
        for _ in 0..10 {
            let mut t = Tracker::new(rules(true, false), bounds.clone(), position.clone()).unwrap();
            run(&mut t, &source);
            landed_new += t.progress().new;
            let next = t.commit();
            if t.progress().new == 0 {
                break;
            }
            position = next;
        }
        assert_eq!(position, Some(Position::At(json!(5))));
        assert_eq!(landed_new, 105, "each record new exactly once");
    }
}
