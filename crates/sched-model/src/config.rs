//! The configuration schema and its loader.
//!
//! The spec types are plain data. A [`Config`] can only be obtained through
//! [`Config::from_yaml`], which validates every reference and policy value first, so a
//! `Config` that exists is a valid one.

use serde::{Deserialize, Serialize};
use serde_yaml_ng::Value;

use crate::ids::{ClassId, JobId, ModelId, ResourceId, SourceId};
use crate::jobs::JobSpec;
use crate::power_params::{SshShutdown, WakeOnLan};
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

/// How a host's power is changed: Wake-on-LAN through the router to power on, SSH with a
/// forced-command key to power off (see `power_params`). Both are required for
/// `control: managed`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerSpec {
    /// Defaults to `manual`.
    #[serde(default)]
    pub control: PowerControl,
    #[serde(default)]
    pub wake_on_lan: Option<WakeOnLan>,
    #[serde(default)]
    pub shutdown: Option<SshShutdown>,
    /// How long the host may take to be observed up after a power-on before the scheduler
    /// gives up, raises an alert and stops (it never retries). Default 10 minutes. Only
    /// meaningful under `control: managed`.
    #[serde(default)]
    pub power_on_timeout: Option<Millis>,
    /// As `power_on_timeout`, for being observed down after a shutdown. Default 15 minutes.
    #[serde(default)]
    pub shutdown_timeout: Option<Millis>,
    /// How long a shutdown may wait for the veto (see [`VetoReading`]: pods still mounting
    /// the host, or still running its models, or the reading being unknown) before the
    /// scheduler gives up, raises an alert and stops (it never retries; a person acts).
    /// Default 30 minutes. Only meaningful under `control: managed`, for a resource that
    /// has a veto reading.
    #[serde(default)]
    pub veto_timeout: Option<Millis>,
    /// The least time flowmanager keeps a host off after it has confirmed a shutdown it
    /// started (the host was observed down) before it powers the host on again by itself, for
    /// a workload, a scheduled job's hold or any other automatic want. A manual lease
    /// overrides it. Default 0: disabled. Only meaningful under `control: managed`.
    #[serde(default)]
    pub min_off: Option<Millis>,
}

/// Default for [`PowerSpec::power_on_timeout`].
pub const DEFAULT_POWER_ON_TIMEOUT: Millis = Millis(10 * 60_000);
/// Default for [`PowerSpec::shutdown_timeout`].
pub const DEFAULT_SHUTDOWN_TIMEOUT: Millis = Millis(15 * 60_000);
/// Default for [`PowerSpec::veto_timeout`].
pub const DEFAULT_VETO_TIMEOUT: Millis = Millis(30 * 60_000);

impl PowerSpec {
    /// The effective power-on timeout.
    pub fn power_on_timeout(&self) -> Millis {
        self.power_on_timeout.unwrap_or(DEFAULT_POWER_ON_TIMEOUT)
    }

    /// The effective veto timeout.
    pub fn veto_timeout(&self) -> Millis {
        self.veto_timeout.unwrap_or(DEFAULT_VETO_TIMEOUT)
    }

    /// The effective shutdown timeout.
    pub fn shutdown_timeout(&self) -> Millis {
        self.shutdown_timeout.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    /// The effective minimum off time (zero: no minimum).
    pub fn min_off(&self) -> Millis {
        self.min_off.unwrap_or(Millis::ZERO)
    }
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
    /// The namespaces the model's workloads run in. The pods veto before a power-off counts
    /// the model as unused only when every one of them is watched by a `pods` source (its
    /// `namespaces`): the observer sees pods of the watched namespaces only, and a workload
    /// elsewhere would read as zero pods without anyone having looked. Without them (or with
    /// one unwatched) the model's pods are unknown to the veto. A managed host whose models are
    /// not covered is refused at load.
    #[serde(default)]
    pub namespaces: Vec<String>,
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
    /// Defaults to the policy's lockout or drain timeout.
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
    Session,
}

impl SourceKind {
    /// Whether this kind of source can technically supply `capability`. A source may only
    /// declare capabilities its kind can supply (checked at load). Webhook receivers only
    /// mark another source dirty and supply nothing themselves.
    pub fn can_supply(self, capability: Capability) -> bool {
        use Capability::*;
        match self {
            SourceKind::KubernetesWatch => {
                matches!(capability, Pods | Reachability | Mounts | Jobs)
            }
            SourceKind::HttpJson | SourceKind::Prometheus => {
                matches!(capability, Reachability | Demand)
            }
            SourceKind::TcpProbe => matches!(capability, Reachability),
            SourceKind::Sql | SourceKind::Session => {
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
    /// Pod counts of models (`model_pods`). Two decisions read them and no others: recovery
    /// waits for a ready pod, and the veto before a power-off of the host waits for zero pods
    /// of every model of the host (Kubernetes enforces GPU exclusivity, so no model switch
    /// waits for pods). A model counts as seen only where its `namespaces` are watched.
    Pods,
    /// Whether a powered host is reachable and up (`resource_up`): kubelet/node readiness
    /// for cluster nodes, a TCP connect probe (or a probe read through Prometheus) for hosts
    /// outside the cluster.
    Reachability,
    /// Queue demand of workload classes (`demand`).
    Demand,
    /// How many pods mount an NFS host (`resource_mounts`): read from the Kubernetes API
    /// against the NFS servers configured per resource. Safety-critical: it is the veto before
    /// that host is powered off, and an unknown or stale reading counts as "possibly mounted".
    Mounts,
    /// The state of the Jobs flowmanager created for its scheduled jobs (`job_runs`): read
    /// from the Kubernetes API. Whether a job is complete, failed or gone; a reading that is
    /// stale or unavailable is never read as "finished".
    Jobs,
}

/// A reading the veto before a power-off consults (see `Config::veto_readings`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VetoReading {
    /// No pod mounts the host's NFS server (the `mounts` capability).
    Mounts,
    /// No pod of the host's models exists (the `pods` capability).
    Pods,
}

impl VetoReading {
    /// The label used in logs, reports and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            VetoReading::Mounts => "mounts",
            VetoReading::Pods => "pods",
        }
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Capability::Pods => "pods",
            Capability::Reachability => "reachability",
            Capability::Demand => "demand",
            Capability::Mounts => "mounts",
            Capability::Jobs => "jobs",
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
    /// How long in-flight jobs may run after a lockout. Defaults to `drain_timeout`.
    #[serde(default)]
    pub lockout_timeout: Option<Millis>,
}

impl PolicyConfig {
    /// The effective lockout timeout.
    pub fn lockout_timeout(&self) -> Millis {
        self.lockout_timeout.unwrap_or(self.drain_timeout)
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
    #[serde(default)]
    jobs: Vec<JobSpec>,
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
    jobs: Vec<JobSpec>,
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
            jobs: raw.jobs,
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

    pub fn jobs(&self) -> &[JobSpec] {
        &self.jobs
    }

    pub fn job(&self, id: &JobId) -> Option<&JobSpec> {
        self.jobs.iter().find(|j| &j.id == id)
    }

    /// The source that reads the state of the scheduled jobs' Kubernetes Jobs: the one that
    /// declares the `jobs` capability (validation allows at most one).
    pub fn jobs_source(&self) -> Option<&SourceSpec> {
        self.sources
            .iter()
            .find(|s| s.observe.contains(&Capability::Jobs))
    }

    /// Whether a lockout lease means anything for `id`: only an exclusive resource with a
    /// power section can be taken back and shut down. Everywhere else a lockout is ignored.
    pub fn supports_lockout(&self, id: &ResourceId) -> bool {
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

    /// Whether a source declaring the `mounts` capability checks which pods mount `id`:
    /// its params name NFS servers for the resource. Such a resource is never powered off
    /// while pods may still mount it.
    pub fn mounts_checked(&self, id: &ResourceId) -> bool {
        self.sources.iter().any(|s| {
            s.observe.contains(&Capability::Mounts)
                && s.kube_watch_params()
                    .ok()
                    .flatten()
                    .is_some_and(|p| p.nfs_servers.get(id).is_some_and(|v| !v.is_empty()))
        })
    }

    /// Whether a source declaring the `pods` capability covers the models of `id`: the host
    /// has models, and a `pods` source reports one count per configured model. Such a
    /// resource is never powered off while a pod of its models may still run.
    pub fn pods_checked(&self, id: &ResourceId) -> bool {
        self.models_of(id).next().is_some()
            && self
                .sources
                .iter()
                .any(|s| s.observe.contains(&Capability::Pods))
    }

    /// Whether the pods of `model` can be seen by a `pods` source: the model names the
    /// namespaces its workloads run in, and every one of them is listed in the `namespaces`
    /// of some source declaring `pods`. "Covered" means this and nothing less; a model that is
    /// not covered reads as unknown to the veto, never as zero pods.
    pub fn model_pods_covered(&self, model: &ModelSpec) -> bool {
        !model.namespaces.is_empty()
            && model.namespaces.iter().all(|ns| {
                self.sources.iter().any(|s| {
                    s.observe.contains(&Capability::Pods)
                        && s.kube_watch_params()
                            .ok()
                            .flatten()
                            .is_some_and(|p| p.namespaces.contains(ns))
                })
            })
    }

    /// The readings the veto before a power-off of `id` needs, in a fixed order. Empty: the
    /// host has no veto.
    pub fn veto_readings(&self, id: &ResourceId) -> Vec<VetoReading> {
        let mut readings = Vec::new();
        if self.mounts_checked(id) {
            readings.push(VetoReading::Mounts);
        }
        if self.pods_checked(id) {
            readings.push(VetoReading::Pods);
        }
        readings
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

    /// The `http_json` sources that asked, with `webhook: { path }`, to be polled when a
    /// request arrives on `path`, in configuration order. Sources whose params do not parse
    /// are left out (validation refuses such a config anyway).
    pub fn webhook_sources(&self, path: &str) -> Vec<&SourceSpec> {
        self.sources
            .iter()
            .filter(|s| {
                s.kind == SourceKind::HttpJson
                    && s.http_json_params()
                        .ok()
                        .flatten()
                        .and_then(|p| p.webhook)
                        .is_some_and(|w| w.path == path)
            })
            .collect()
    }

    /// Every webhook path some source declares, sorted and without repeats.
    pub fn webhook_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .sources
            .iter()
            .filter(|s| s.kind == SourceKind::HttpJson)
            .filter_map(|s| s.http_json_params().ok().flatten()?.webhook)
            .map(|w| w.path)
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    /// Models belonging to `resource`, in configuration order.
    pub fn models_of<'a>(
        &'a self,
        resource: &'a ResourceId,
    ) -> impl Iterator<Item = &'a ModelSpec> {
        self.models.iter().filter(move |m| &m.resource == resource)
    }
}
