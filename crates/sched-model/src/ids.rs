//! Typed identifiers. Ids are opaque names from configuration; no crate interprets them.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(id: &str) -> Self {
                Self::new(id)
            }
        }
    };
}

id_type!(
    /// A resource: an exclusive-mode host (the GPU node) or a shared one (the NFS share).
    ResourceId
);
id_type!(
    /// A mode of an exclusive resource.
    ModelId
);
id_type!(
    /// A unit of demand with its own requirements and priority.
    ClassId
);
id_type!(
    /// A configured observation source.
    SourceId
);
id_type!(
    /// A lease record.
    LeaseId
);
id_type!(
    /// Whoever holds a shared reference-counted lease.
    HolderId
);
