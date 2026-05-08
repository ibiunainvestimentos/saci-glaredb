use pgrepr::notice::NoticeSeverity;

use super::{split_comma_delimited, Dialect, Display, FromStr, ToOwned, Uuid};

pub trait Value: ToOwned + std::fmt::Debug {
    fn try_parse(s: &str) -> Option<Self::Owned>;
    fn format(&self) -> String;
}

impl Value for str {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(s.to_string())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for String {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(s.to_string())
    }

    fn format(&self) -> String {
        self.clone()
    }
}

impl Value for bool {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        // Match Postgres `parse_bool` semantics — GUC SETs accept any of
        // these forms, case-insensitively. Critical for round-trips with
        // PgJDBC / psql, which write `on`/`off` and read it back.
        match s.to_ascii_lowercase().as_str() {
            "t" | "true" | "on" | "yes" | "1" => Some(true),
            "f" | "false" | "off" | "no" | "0" => Some(false),
            _ => None,
        }
    }

    fn format(&self) -> String {
        // Postgres formats boolean GUCs as `on`/`off` — both in
        // `SHOW <var>` results and in the `ParameterStatus` wire message
        // sent at startup. PgJDBC's `setupServerParameters` rejects any
        // other rendering (it raises "could not parse server response: …
        // expected on or off, got <value>"), and DBeaver / pgcli /
        // psqlODBC behave the same way. The integer/string/uuid bools
        // elsewhere in the catalog (e.g. `pg_attribute.attnotnull`) keep
        // their `t`/`f` rendering — those are typed BOOLEAN columns,
        // not GUCs, and go through the pgrepr Scalar layer instead of
        // this `Value` trait.
        if *self { "on" } else { "off" }.to_string()
    }
}

impl Value for i32 {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for usize {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for Uuid {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl<V> Value for Option<V>
where
    V: Value + ?Sized + 'static + ToOwned + Clone + FromStr + Display,
{
    fn try_parse(s: &str) -> Option<Self::Owned> {
        let v = s.parse::<V>().ok()?;
        Some(Some(v))
    }

    fn format(&self) -> String {
        match self {
            Some(v) => v.to_string(),
            None => "None".to_string(),
        }
    }
}

impl Value for [String] {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(split_comma_delimited(s))
    }

    fn format(&self) -> String {
        self.join(",")
    }
}

impl Value for Dialect {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        match s {
            "sql" => Some(Dialect::Sql),
            "prql" => Some(Dialect::Prql),
            _ => None,
        }
    }

    fn format(&self) -> String {
        match self {
            Dialect::Sql => "sql".to_string(),
            Dialect::Prql => "prql".to_string(),
        }
    }
}

impl Value for NoticeSeverity {
    fn try_parse(s: &str) -> Option<NoticeSeverity> {
        NoticeSeverity::from_str(s).ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}
