//! The statement a table read runs, from its position.

use shapeshift_core::Position;

/// A column name as one identifier, a backtick in it doubled: a name read from the server
/// (a primary key's) is not checked, and must not end the quoting.
pub(crate) fn column(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// A table name (checked: letters, digits, `_`, `$`), `db.table` quoted per part.
pub(crate) fn quote(name: &str) -> String {
    name.split('.')
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Where a table read starts.
pub(crate) enum From<'a> {
    /// The first run, with no `start`: every row.
    Everything,
    /// Rows at or above (`exclusive`: above) the bound bound as `:v`.
    AtOrAbove { exclusive: bool },
    /// `inside v, after k`: rows above `v`, and those at `v` with a key above `k`.
    Inside { key: &'a str },
    /// The same with a lookback: also the rows from the cutoff `:c` up to `v`.
    InsideReplaying { key: &'a str },
}

/// The read of `table` by `cursor`, ordered by it and the primary key. A row whose cursor is
/// NULL has no place in the order, and is not read.
pub(crate) fn table_read(
    table: &str,
    cursor: &str,
    primary_key: &[String],
    from: From<'_>,
) -> String {
    let w = column(cursor);
    let cond = match from {
        From::Everything => String::new(),
        From::AtOrAbove { exclusive: false } => format!(" AND {w} >= :v"),
        From::AtOrAbove { exclusive: true } => format!(" AND {w} > :v"),
        From::Inside { key } => {
            let k = column(key);
            format!(" AND ({w} > :v OR ({w} = :v AND {k} > :k))")
        }
        From::InsideReplaying { key } => {
            let k = column(key);
            format!(" AND {w} >= :c AND NOT ({w} = :v AND {k} <= :k)")
        }
    };
    let mut order = vec![w.clone()];
    order.extend(
        primary_key
            .iter()
            .filter(|c| *c != cursor)
            .map(|c| column(c)),
    );
    format!(
        "SELECT * FROM {} WHERE {w} IS NOT NULL{cond} ORDER BY {}",
        quote(table),
        order.join(", ")
    )
}

/// Which [`From`] a position and a `start` give, for a table whose tiebreak is `key`.
pub(crate) fn from_position<'a>(
    position: Option<&Position>,
    strict: bool,
    has_start: bool,
    key: Option<&'a str>,
    lookback: bool,
) -> From<'a> {
    match (position, key) {
        (Some(Position::Inside(..)), Some(key)) if lookback => From::InsideReplaying { key },
        (Some(Position::Inside(..)), Some(key)) => From::Inside { key },
        (Some(Position::At(_)), _) => From::AtOrAbove { exclusive: strict },
        // A position inside a group, and no single-column key to resume by now: the group is
        // read again (a merge key keeps it once).
        (Some(Position::Inside(..)), None) => From::AtOrAbove { exclusive: false },
        (None, _) if has_start => From::AtOrAbove { exclusive: false },
        (None, _) => From::Everything,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_column_name_cannot_end_its_quoting() {
        assert_eq!(column("a`b"), "`a``b`");
        assert_eq!(column("a.b"), "`a.b`");
        let pk = vec!["x`; DROP TABLE t; --".to_string()];
        let sql = table_read("t", "w", &pk, From::Everything);
        assert!(
            sql.ends_with("ORDER BY `w`, `x``; DROP TABLE t; --`"),
            "{sql}"
        );
    }

    #[test]
    fn a_resume_inside_a_group_with_a_lookback_reads_from_the_cutoff() {
        let pk = vec!["id".to_string()];
        let inside = Position::Inside(json!("x"), json!(9));
        assert!(matches!(
            from_position(Some(&inside), false, false, Some("id"), true),
            From::InsideReplaying { .. }
        ));
        assert_eq!(
            table_read("t", "u", &pk, From::InsideReplaying { key: "id" }),
            "SELECT * FROM `t` WHERE `u` IS NOT NULL AND `u` >= :c AND NOT (`u` = :v AND `id` <= :k) ORDER BY `u`, `id`"
        );
    }

    #[test]
    fn a_table_read_follows_its_position() {
        let pk = vec!["id".to_string()];
        assert_eq!(
            table_read("orders", "updated_at", &pk, From::Everything),
            "SELECT * FROM `orders` WHERE `updated_at` IS NOT NULL ORDER BY `updated_at`, `id`"
        );
        assert_eq!(
            table_read("shop.orders", "updated_at", &pk, From::Inside { key: "id" }),
            "SELECT * FROM `shop`.`orders` WHERE `updated_at` IS NOT NULL AND (`updated_at` > :v OR \
             (`updated_at` = :v AND `id` > :k)) ORDER BY `updated_at`, `id`"
        );
        // A cursor that is the key itself is ordered once.
        assert_eq!(
            table_read("t", "id", &pk, From::AtOrAbove { exclusive: true }),
            "SELECT * FROM `t` WHERE `id` IS NOT NULL AND `id` > :v ORDER BY `id`"
        );
        let at = Position::At(json!(3));
        assert!(matches!(
            from_position(Some(&at), true, false, Some("id"), false),
            From::AtOrAbove { exclusive: true }
        ));
        let inside = Position::Inside(json!(3), json!(9));
        assert!(matches!(
            from_position(Some(&inside), false, false, Some("id"), false),
            From::Inside { .. }
        ));
        assert!(matches!(
            from_position(None, false, true, None, false),
            From::AtOrAbove { exclusive: false }
        ));
        assert!(matches!(
            from_position(None, false, false, None, false),
            From::Everything
        ));
    }
}
