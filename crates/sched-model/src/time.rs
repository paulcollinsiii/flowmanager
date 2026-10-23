//! Explicit time. The pure crates never read a clock: instants and durations are
//! values carried in the data, so decisions are reproducible from a recorded snapshot.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Milliseconds: a unix timestamp when used as an instant, a length when used as a duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Millis(pub u64);

impl Millis {
    pub const ZERO: Millis = Millis(0);

    pub const fn from_secs(secs: u64) -> Self {
        Millis(secs.saturating_mul(1000))
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn saturating_sub(self, other: Millis) -> Millis {
        Millis(self.0.saturating_sub(other.0))
    }

    pub fn saturating_add(self, other: Millis) -> Millis {
        Millis(self.0.saturating_add(other.0))
    }

    /// Parses `1500`, `1500ms`, `30s`, `5m`, `2h` or `1d`: ASCII digits immediately
    /// followed by an optional unit, no spaces or separators. A bare number is milliseconds.
    pub fn parse(text: &str) -> Result<Millis, String> {
        const HINT: &str = "expected digits with an optional unit (ms, s, m, h, d), e.g. \"30s\"; no spaces, signs or separators";
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len());
        let (digits, unit) = text.split_at(split);
        let value: u64 = digits
            .parse()
            .map_err(|_| format!("invalid duration {text:?}: {HINT}"))?;
        let factor: u64 = match unit {
            "" | "ms" => 1,
            "s" => 1000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => {
                return Err(format!(
                    "invalid duration {text:?}: unknown unit {unit:?}; {HINT}"
                ));
            }
        };
        value
            .checked_mul(factor)
            .map(Millis)
            .ok_or_else(|| format!("duration {text:?} is too large"))
    }
}

impl fmt::Display for Millis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

impl Serialize for Millis {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for Millis {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Millis;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration: a whole number of milliseconds or a string like \"30s\" (units ms, s, m, h, d)")
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Millis, E> {
                Ok(Millis(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Millis, E> {
                u64::try_from(v)
                    .map(Millis)
                    .map_err(|_| E::custom(format!("duration {v} must not be negative")))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Millis, E> {
                Millis::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_units() {
        assert_eq!(Millis::parse("250"), Ok(Millis(250)));
        assert_eq!(Millis::parse("250ms"), Ok(Millis(250)));
        assert_eq!(Millis::parse("30s"), Ok(Millis(30_000)));
        assert_eq!(Millis::parse("5m"), Ok(Millis(300_000)));
        assert_eq!(Millis::parse("2h"), Ok(Millis(7_200_000)));
        assert_eq!(Millis::parse("1d"), Ok(Millis(86_400_000)));
    }

    #[test]
    fn rejects_bad_durations() {
        assert!(Millis::parse("").is_err());
        assert!(Millis::parse("abc").is_err());
        assert!(Millis::parse("5x").is_err());
        assert!(Millis::parse("-5s").is_err());
        assert!(Millis::parse("99999999999999999999d").is_err());
        assert!(Millis::parse("5 m").is_err());
        assert!(Millis::parse(" 5m").is_err());
        assert!(
            Millis::parse("1_000")
                .unwrap_err()
                .contains("no spaces, signs or separators")
        );
    }

    #[test]
    fn from_secs_saturates() {
        assert_eq!(Millis::from_secs(u64::MAX), Millis(u64::MAX));
    }

    #[test]
    fn yaml_rejects_non_durations_with_clear_message() {
        for bad in ["true", "1.5", "-3", "null", "[1]"] {
            let err = serde_yaml_ng::from_str::<Millis>(bad)
                .unwrap_err()
                .to_string();
            assert!(!err.contains("untagged"), "{bad}: {err}");
        }
        assert!(
            serde_yaml_ng::from_str::<Millis>("-3")
                .unwrap_err()
                .to_string()
                .contains("must not be negative")
        );
        assert!(
            serde_yaml_ng::from_str::<Millis>("true")
                .unwrap_err()
                .to_string()
                .contains("expected a duration")
        );
    }

    #[test]
    fn yaml_accepts_number_and_text() {
        let a: Millis = serde_yaml_ng::from_str("1500").unwrap();
        let b: Millis = serde_yaml_ng::from_str("\"2s\"").unwrap();
        assert_eq!((a, b), (Millis(1500), Millis(2000)));
    }

    proptest! {
        #[test]
        fn seconds_text_matches_number(n in 0u64..1_000_000_000) {
            prop_assert_eq!(Millis::parse(&format!("{n}s")), Ok(Millis(n * 1000)));
        }

        #[test]
        fn yaml_roundtrip(n in any::<u64>()) {
            let text = serde_yaml_ng::to_string(&Millis(n)).unwrap();
            let back: Millis = serde_yaml_ng::from_str(&text).unwrap();
            prop_assert_eq!(back, Millis(n));
        }
    }
}
