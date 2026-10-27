//! The configuration schema and its loader.
//!
//! The spec types are plain data. A [`Config`] can only be obtained through
//! [`Config::from_yaml`], which validates every reference and policy value first, so a
//! `Config` that exists is a valid one.

use serde::{Deserialize, Serialize};
use serde_yaml_ng::Value;

use crate::ids::{ClassId, ModelId, ResourceId, SourceId};
use crate::time::Millis;
use crate::validate::{self, Problem};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// Holds at most one mode (model) at a time.
    Exclusive,
    /// Any number of holders, tracked by reference count.
    Shared,
}

/// Who changes a host's power.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PowerControl {
    /// A person powers the host on and off. The scheduler never emits a power step for it
    /// and never takes responsibility for its power; its power phase follows observed
    /// reachability (on or off). Work that needs the host waits until it is up.
    #[default]
    Manual,
    /// The scheduler powers the host on (manual requests, power-changing classes) and off
    /// again once nothing holds it, through the actuator.
    Managed,
}

/// How a host's power is changed. The actuator kind lives in code; its parameters here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerSpec {
    pub actuator: String,
    #[serde(default)]
    pub params: Value,
    /// Defaults to `manual`.
    #[serde(default)]
    pub control: PowerControl,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub id: ResourceId,
    pub kind: ResourceKind,
    /// Absent for resources the scheduler never powers (always on).
    #[serde(default)]
    pub power: Option<PowerSpec>,
}

/// A mode of an exclusive resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
    pub id: ModelId,
    pub resource: ResourceId,
    /// Estimated time to load; used as the cost of switching to this model.
    pub load_time: Millis,
    /// Names of the scaled workloads that implement this model.
    pub workloads: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Preemptibility {
    /// Must not be interrupted: while it has work, the scheduler does not switch away,
    /// and that wait has no timeout. The only unbounded case.
    None,
    /// Intake can be paused and in-flight work is given until the timeout; then it is cut
    /// off (the model is withdrawn). Nothing re-queues it unless the class has a recovery.
    PauseIntake,
    /// As `PauseIntake`, for work that can be killed and safely retried (by the workload
    /// itself, or by the class's recovery). The scheduler treats both the same way today.
    Kill,
}

/// One resource a class needs, and for an exclusive resource, possibly which model.
///
/// - Exclusive resource with a model: that model must be serving (and not winding down).
/// - Exclusive resource without a model: only that the host is up. This makes the class a
///   *gate class*: it must have no demand source and never influences model choice (for
///   example a consumer that may run only while the GPU host and the NFS share are up).
/// - Shared resource (a model is forbidden): the host is up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub resource: ResourceId,
    /// Optional for exclusive resources (absent: the host is up), forbidden for shared ones.
    #[serde(default)]
    pub model: Option<ModelId>,
}

/// Where a class's demand is observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemandRef {
    pub source: SourceId,
}

/// An action to run after in-flight work is killed. The kind lives in code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySpec {
    pub action: String,
    /// Source the action works against (for example, where failed items are listed).
    /// Validated to exist. `params` must not carry references to other entries.
    #[serde(default)]
    pub source: Option<SourceId>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassSpec {
    pub id: ClassId,
    pub requires: Vec<Requirement>,
    /// Higher wins when choosing which ready model to load next.
    pub priority: u32,
    pub preemptibility: Preemptibility,
    /// Whether this class may cause its resources to be powered on or off.
    #[serde(default)]
    pub may_change_power: bool,
    /// Absent for classes with no demand of their own; they run whenever their
    /// requirements are up (for example services that only need the NFS share, or gate
    /// classes on host readiness). Forbidden for a class that requires an exclusive
    /// resource without a model (a gate class).
    #[serde(default)]
    pub demand: Option<DemandRef>,
    #[serde(default)]
    pub recovery: Option<RecoverySpec>,
    /// How long this class's in-flight work may run once a wind-down starts (forced or not).
    /// Defaults to the policy's reclaim or drain timeout.
    #[serde(default)]
    pub kill_timeout: Option<Millis>,
}

/// Adapter kinds. Implementations live in `sched-observe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    KubernetesWatch,
    HttpJson,
    Sql,
    Prometheus,
    /// A TCP connect to one host and port, for the reachability of one resource.
    TcpProbe,
    Webhook,
    Schedule,
    Session,
}

impl SourceKind {
    /// Whether this kind of source can technically supply `capability`. A source may only
    /// declare capabilities its kind can supply (checked at load). Webhook receivers only
    /// mark another source dirty and supply nothing themselves.
    pub fn can_supply(self, capability: Capability) -> bool {
        use Capability::*;
        match self {
            SourceKind::KubernetesWatch => matches!(capability, Pods | Reachability),
            SourceKind::HttpJson | SourceKind::Prometheus => {
                matches!(capability, Reachability | Demand)
            }
            SourceKind::TcpProbe => matches!(capability, Reachability),
            SourceKind::Sql | SourceKind::Schedule | SourceKind::Session => {
                matches!(capability, Demand)
            }
            SourceKind::Webhook => false,
        }
    }
}

/// What a source is declared to report. This declaration, not the source's kind, decides
/// which observations the scheduler accepts from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Pod counts of models (`model_pods`). Information only: no decision reads them
    /// (Kubernetes enforces GPU exclusivity).
    Pods,
    /// Whether a powered host is reachable and up (`resource_up`): kubelet/node readiness
    /// for cluster nodes, a TCP connect probe (or a probe read through Prometheus) for hosts
    /// outside the cluster.
    Reachability,
    /// Queue demand of workload classes (`demand`).
    Demand,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Capability::Pods => "pods",
            Capability::Reachability => "reachability",
            Capability::Demand => "demand",
        })
    }
}

/// An adapter instance. `params` is opaque here; the adapter kind interprets it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    pub id: SourceId,
    pub kind: SourceKind,
    /// Interval of the slow periodic resync that backs up events.
    pub resync: Millis,
    /// What this source reports. Observations of a kind it does not declare are ignored.
    #[serde(default)]
    pub observe: std::collections::BTreeSet<Capability>,
    #[serde(default)]
    pub params: Value,
}

/// Longest accepted `interactive_ttl` and `drain_timeout`.
pub const MAX_LEASE_DURATION: Millis = Millis(86_400_000);

/// Tunable scheduling parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    /// Name of the planner strategy.
    pub strategy: String,
    /// Minimum time a model stays allowed before it may be switched away from, counted from
    /// the tick it was allowed: it includes the time its pods take to load.
    pub min_dwell: Millis,
    /// Maximum time a model keeps serving while other queues wait.
    pub max_dwell: Millis,
    /// Idle time before unloading a model or releasing a host.
    pub idle_grace: Millis,
    /// Age of the oldest ready item on another queue that forces a switch.
    pub age_threshold: Millis,
    /// Oldest acceptable observation for scheduling inputs such as queue depth.
    pub queue_reading_stale_after: Millis,
    /// Oldest acceptable observation for safety-critical checks (host reachability, before
    /// a model is allowed or a host powered off).
    pub host_reading_stale_after: Millis,
    /// Lifetime of an interactive lease before it must be renewed.
    pub interactive_ttl: Millis,
    /// How long in-flight jobs may run after a manual model lease cuts batch work short.
    pub drain_timeout: Millis,
    /// How long in-flight jobs may run after a reclaim. Defaults to `drain_timeout`.
    #[serde(default)]
    pub reclaim_timeout: Option<Millis>,
}

impl PolicyConfig {
    /// The effective reclaim timeout.
    pub fn reclaim_timeout(&self) -> Millis {
        self.reclaim_timeout.unwrap_or(self.drain_timeout)
    }

    /// The oldest an observation of this kind may be and still be used: safety-critical
    /// kinds are held to `host_reading_stale_after`, scheduling inputs to `queue_reading_stale_after`.
    pub fn max_age_for(&self, value: &crate::observation::ObservedValue) -> Millis {
        if value.is_safety_critical() {
            self.host_reading_stale_after
        } else {
            self.queue_reading_stale_after
        }
    }
}

/// Unvalidated shape of the configuration file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    policy: PolicyConfig,
    #[serde(default)]
    resources: Vec<ResourceSpec>,
    #[serde(default)]
    models: Vec<ModelSpec>,
    #[serde(default)]
    classes: Vec<ClassSpec>,
    #[serde(default)]
    sources: Vec<SourceSpec>,
}

/// A validated configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    generation: u64,
    policy: PolicyConfig,
    resources: Vec<ResourceSpec>,
    models: Vec<ModelSpec>,
    classes: Vec<ClassSpec>,
    sources: Vec<SourceSpec>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot parse configuration: {0}")]
    Parse(#[from] serde_yaml_ng::Error),
    #[error("{}", format_problems(.0))]
    Invalid(Vec<Problem>),
}

fn format_problems(problems: &[Problem]) -> String {
    let mut out = format!("invalid configuration ({} problem(s)):", problems.len());
    for p in problems {
        out.push_str("\n  - ");
        out.push_str(&p.to_string());
    }
    out
}

impl Config {
    /// Parses and validates. Returns every problem found, never a partially valid value.
    pub fn from_yaml(text: &str) -> Result<Config, ConfigError> {
        Self::from_yaml_with_generation(text, 0)
    }

    /// As [`Config::from_yaml`], stamping the config generation that ticks are pinned to.
    pub fn from_yaml_with_generation(text: &str, generation: u64) -> Result<Config, ConfigError> {
        let raw: RawConfig = serde_yaml_ng::from_str(text)?;
        let config = Config {
            generation,
            policy: raw.policy,
            resources: raw.resources,
            models: raw.models,
            classes: raw.classes,
            sources: raw.sources,
        };
        let problems = validate::validate(&config);
        if problems.is_empty() {
            Ok(config)
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// The generation of this config; a snapshot must carry the same value.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn policy(&self) -> &PolicyConfig {
        &self.policy
    }

    pub fn resources(&self) -> &[ResourceSpec] {
        &self.resources
    }

    pub fn models(&self) -> &[ModelSpec] {
        &self.models
    }

    pub fn classes(&self) -> &[ClassSpec] {
        &self.classes
    }

    pub fn sources(&self) -> &[SourceSpec] {
        &self.sources
    }

    /// Whether a reclaim lease means anything for `id`: only an exclusive resource with a
    /// power section can be taken back and shut down. Everywhere else a reclaim is ignored.
    pub fn supports_reclaim(&self, id: &ResourceId) -> bool {
        self.resource(id)
            .is_some_and(|r| r.kind == ResourceKind::Exclusive && r.power.is_some())
    }

    /// Whether the scheduler controls `id`'s power: it has a power section with
    /// `control: managed`.
    pub fn power_managed(&self, id: &ResourceId) -> bool {
        self.resource(id)
            .and_then(|r| r.power.as_ref())
            .is_some_and(|p| p.control == PowerControl::Managed)
    }

    pub fn resource(&self, id: &ResourceId) -> Option<&ResourceSpec> {
        self.resources.iter().find(|r| &r.id == id)
    }

    pub fn model(&self, id: &ModelId) -> Option<&ModelSpec> {
        self.models.iter().find(|m| &m.id == id)
    }

    pub fn class(&self, id: &ClassId) -> Option<&ClassSpec> {
        self.classes.iter().find(|c| &c.id == id)
    }

    /// Whether `id` is a configured source that declares `capability`. Observations of that
    /// kind from any other source are ignored.
    pub fn source_observes(&self, id: &SourceId, capability: Capability) -> bool {
        self.source(id)
            .is_some_and(|s| s.observe.contains(&capability))
    }

    pub fn source(&self, id: &SourceId) -> Option<&SourceSpec> {
        self.sources.iter().find(|s| &s.id == id)
    }

    /// Models belonging to `resource`, in configuration order.
    pub fn models_of<'a>(
        &'a self,
        resource: &'a ResourceId,
    ) -> impl Iterator<Item = &'a ModelSpec> {
        self.models.iter().filter(move |m| &m.resource == resource)
    }
}
