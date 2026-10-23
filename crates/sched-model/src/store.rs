//! The active configuration and its generation. Invalid configuration never replaces the
//! last good one.

use crate::config::{Config, ConfigError};

/// Holds the last good [`Config`]. Pure: callers hand in the new YAML; nothing is read
/// or watched here.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    active: Config,
}

impl ConfigStore {
    /// Starts at generation 1. There is no last good config to fall back to, so this fails
    /// if the first config is invalid.
    pub fn new(yaml: &str) -> Result<ConfigStore, ConfigError> {
        Ok(ConfigStore {
            active: Config::from_yaml_with_generation(yaml, 1)?,
        })
    }

    pub fn active(&self) -> &Config {
        &self.active
    }

    pub fn generation(&self) -> u64 {
        self.active.generation()
    }

    /// Validates `yaml` in full and, only if it is valid, makes it the active config with
    /// the next generation (every successful apply bumps it, even if the text is
    /// unchanged). On any error the active config and generation are untouched.
    pub fn apply(&mut self, yaml: &str) -> Result<u64, ConfigError> {
        let next = Config::from_yaml_with_generation(yaml, self.generation().saturating_add(1))?;
        let generation = next.generation();
        self.active = next;
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::EXAMPLE_CONFIG;

    #[test]
    fn apply_bumps_generation_on_success() {
        let mut s = ConfigStore::new(EXAMPLE_CONFIG).unwrap();
        assert_eq!(s.generation(), 1);
        let changed = EXAMPLE_CONFIG.replace("idle_grace: 10m", "idle_grace: 20m");
        assert_eq!(s.apply(&changed).unwrap(), 2);
        assert_eq!(s.active().policy().idle_grace.as_u64(), 20 * 60_000);
        assert_eq!(s.apply(&changed).unwrap(), 3);
    }

    #[test]
    fn invalid_input_leaves_the_active_config_untouched() {
        let mut s = ConfigStore::new(EXAMPLE_CONFIG).unwrap();
        let before = s.active().clone();
        // Unparseable yaml, an unknown field, and a validation failure.
        for bad in [
            "policy: [".to_string(),
            EXAMPLE_CONFIG.replace("priority: 50", "priorty: 50"),
            EXAMPLE_CONFIG.replace("min_dwell: 5m", "min_dwell: 3h"),
            EXAMPLE_CONFIG.replace("{ resource: basestar-nfs }", "{ resource: nowhere }"),
        ] {
            assert!(s.apply(&bad).is_err());
            assert_eq!(s.generation(), 1);
            assert_eq!(s.active(), &before);
        }
        // And a good one still applies afterwards.
        assert_eq!(s.apply(EXAMPLE_CONFIG).unwrap(), 2);
    }

    #[test]
    fn first_config_must_be_valid() {
        assert!(ConfigStore::new("policy: [").is_err());
    }
}
