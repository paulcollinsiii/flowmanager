use std::collections::BTreeSet;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Node, PersistentVolume, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ManagedFieldsEntry;
use kube::ResourceExt;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams};
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};

use crate::cluster::{ClusterError, Drain, Server, list_servers};
use crate::power::{PowerObjectClient, shutdown_target};

/// Label on a ScaledObject naming the group it belongs to.
const L_GROUP: &str = "power.kaffi.internal/group";
/// Optional label naming which role of its group a ScaledObject serves.
const L_ROLE: &str = "power.kaffi.internal/gpu-role";
/// KEDA pauses a ScaledObject at this many replicas while the annotation is set.
const PAUSE: &str = "autoscaling.keda.sh/paused-replicas";
const FIELD_MANAGER: &str = "flowmanager";
const FLUX_MANAGER: &str = "kustomize-controller";
const GPU_NODE_LABEL: &str = "kaffi.internal/gpu=nvidia";
const GPU_RESOURCE: &str = "nvidia.com/gpu";
const DRAIN_POLL: Duration = Duration::from_secs(5);
/// Words that mean something as a mode, so a role may not use them.
const RESERVED_MODES: [&str; 3] = ["off", "auto", "on"];

/// A ScaledObject that gates a workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gate {
    pub namespace: String,
    pub name: String,
    pub group: String,
    pub role: Option<String>,
    /// Current value of the pause annotation, if any.
    pub paused: Option<String>,
    /// Flux owns the pause annotation (it is in git), so changing it would be reverted.
    pub flux_owned: bool,
}

impl Gate {
    fn id(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// A group of gates, and the server they depend on if there is one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub name: String,
    pub server: Option<Server>,
    pub roles: BTreeSet<String>,
    pub gates: Vec<Gate>,
}

/// What the catalog shows about a group.
#[derive(Serialize, Deserialize)]
pub struct GroupView {
    name: String,
    server: Option<String>,
    exclusive_roles: bool,
    /// Every mode set_mode accepts for the group, found from the cluster.
    modes: Vec<String>,
    gates: Vec<Gate>,
}

impl From<&Group> for GroupView {
    fn from(g: &Group) -> Self {
        let mut modes = vec!["off".to_string(), "auto".to_string()];
        modes.extend(g.roles.iter().cloned());
        Self {
            name: g.name.clone(),
            server: g.server.as_ref().map(|s| s.name.clone()),
            exclusive_roles: g.server.as_ref().is_some_and(|s| s.exclusive_roles),
            modes,
            gates: g.gates.clone(),
        }
    }
}

/// off: pause the group and power its server down. auto: power the server up and
/// leave scaling to KEDA. A role: power up and hold that role at one replica.
#[derive(Debug, Clone, PartialEq)]
enum Mode {
    Off,
    Auto,
    Role(String),
}

impl Mode {
    fn parse(s: &str) -> Self {
        match s.trim() {
            "off" => Self::Off,
            "auto" | "on" => Self::Auto,
            role => Self::Role(role.to_string()),
        }
    }
}

/// A request the cluster state rules out. Nothing has been changed when one is returned.
#[derive(Debug, PartialEq)]
struct Refusal {
    code: u16,
    message: String,
}

fn refuse<T>(code: u16, message: String) -> Result<T, Refusal> {
    Err(Refusal { code, message })
}

impl From<Refusal> for HandlerError {
    fn from(r: Refusal) -> Self {
        TerminalError::new_with_code(r.code, r.message).into()
    }
}

#[derive(Debug, Default, PartialEq)]
struct Plan {
    pause: Vec<Gate>,
    hold: Vec<Gate>,
    unpause: Vec<Gate>,
}

/// True if Flux's field manager owns the pause annotation.
fn owned_by_flux(fields: &[ManagedFieldsEntry]) -> bool {
    let key = format!("f:{PAUSE}");
    fields.iter().any(|mf| {
        mf.manager.as_deref() == Some(FLUX_MANAGER)
            && mf.fields_v1.as_ref().is_some_and(|f| f.0["f:metadata"]["f:annotations"].get(&key).is_some())
    })
}

fn parse_gate(obj: &DynamicObject) -> Result<Gate, String> {
    let name = obj.name_any();
    let group = obj.labels().get(L_GROUP).ok_or_else(|| format!("{name} has no {L_GROUP} label"))?.clone();
    Ok(Gate {
        namespace: obj.namespace().ok_or_else(|| format!("{name} has no namespace"))?,
        name,
        group,
        role: obj.labels().get(L_ROLE).cloned(),
        paused: obj.annotations().get(PAUSE).cloned(),
        flux_owned: owned_by_flux(obj.managed_fields()),
    })
}

/// Gates come from the cluster's labels and servers from its ConfigMaps; a group
/// is whatever the gates name, tied to the one server that lists it.
fn build_groups(servers: &[Server], gates: Vec<Gate>) -> Result<Vec<Group>, String> {
    let names: BTreeSet<&str> = gates.iter().map(|g| g.group.as_str()).collect();
    let mut groups = Vec::new();
    for name in names {
        let serving: Vec<&Server> = servers.iter().filter(|s| s.groups.iter().any(|g| g == name)).collect();
        if serving.len() > 1 {
            let who: Vec<&str> = serving.iter().map(|s| s.name.as_str()).collect();
            return Err(format!("group {name} is served by more than one server: {}", who.join(", ")));
        }
        let members: Vec<Gate> = gates.iter().filter(|g| g.group == name).cloned().collect();
        groups.push(Group {
            name: name.to_string(),
            server: serving.first().map(|s| (*s).clone()),
            roles: members.iter().filter_map(|g| g.role.clone()).collect(),
            gates: members,
        });
    }
    Ok(groups)
}

/// The paused-replicas a gate should carry: "0" paused, "1" held up, None for KEDA to decide.
fn wanted_replicas(mode: &Mode, exclusive: bool, gate: &Gate) -> Option<&'static str> {
    match mode {
        Mode::Off => Some("0"),
        Mode::Auto => None,
        Mode::Role(role) => match gate.role.as_deref() {
            // Not a user of the contended resource itself: up whenever the server is.
            None => None,
            Some(r) if r == role => Some("1"),
            Some(_) if exclusive => Some("0"),
            Some(_) => None,
        },
    }
}

/// Decide what to change, and refuse anything that cannot work, before any mutation.
fn plan_mode(group: &Group, mode: &Mode, all: &[Group]) -> Result<Plan, Refusal> {
    if let Some(bad) = group.roles.iter().find(|r| RESERVED_MODES.contains(&r.as_str())) {
        return refuse(422, format!("group {}: role {bad:?} clashes with a mode name", group.name));
    }
    if let Mode::Role(role) = mode
        && !group.roles.contains(role)
    {
        let mut modes = vec!["off".to_string(), "auto".to_string()];
        modes.extend(group.roles.iter().cloned());
        return refuse(400, format!("group {}: unknown mode {role:?}; available: {}", group.name, modes.join(", ")));
    }

    let exclusive = group.server.as_ref().is_some_and(|s| s.exclusive_roles);
    let mut plan = Plan::default();
    for gate in &group.gates {
        let want = wanted_replicas(mode, exclusive, gate);
        if want == gate.paused.as_deref() {
            continue;
        }
        if gate.flux_owned {
            return refuse(
                409,
                format!(
                    "{}: {PAUSE}={} is owned by Flux (it is in git) and would be reverted; remove it from git first",
                    gate.id(),
                    gate.paused.as_deref().unwrap_or("-"),
                ),
            );
        }
        match want {
            Some("0") => plan.pause.push(gate.clone()),
            Some(_) => plan.hold.push(gate.clone()),
            None => plan.unpause.push(gate.clone()),
        }
    }

    if let (Mode::Off, Some(server)) = (mode, &group.server) {
        if server.drain.is_none() {
            return refuse(412, format!("server {}: DRAIN is required to power off", server.name));
        }
        if let Err(e) = shutdown_target(server) {
            return refuse(412, format!("server {}: {e}", server.name));
        }
        // One server may back several groups; do not pull it from under a live one.
        for other in all.iter().filter(|o| o.name != group.name) {
            let shares = other.server.as_ref().is_some_and(|s| s.name == server.name);
            if shares && other.gates.iter().any(|g| g.paused.as_deref() != Some("0")) {
                return refuse(
                    409,
                    format!("server {} also serves group {}, which is not off; turn that off first", server.name, other.name),
                );
            }
        }
    }
    Ok(plan)
}

fn describe(pod: &Pod) -> String {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref()).unwrap_or("?");
    let terminating = if pod.metadata.deletion_timestamp.is_some() { " Terminating" } else { "" };
    format!("{}/{} {phase}{terminating}", pod.namespace().unwrap_or_default(), pod.name_any())
}

fn is_live(pod: &Pod) -> bool {
    !matches!(pod.status.as_ref().and_then(|s| s.phase.as_deref()), Some("Succeeded" | "Failed"))
}

/// A pod that mounts a PV backed by NFS. An inline `nfs:` volume names a server
/// directly with no PV; count it whichever server it names, to be conservative.
fn mounts_nfs(pod: &Pod, nfs_claims: &BTreeSet<String>) -> bool {
    let ns = pod.namespace().unwrap_or_default();
    let volumes = pod.spec.iter().flat_map(|s| s.volumes.iter().flatten());
    volumes.into_iter().any(|v| {
        v.nfs.is_some()
            || v.persistent_volume_claim.as_ref().is_some_and(|p| nfs_claims.contains(&format!("{ns}/{}", p.claim_name)))
    })
}

/// A pod that asks for a GPU or runs on a GPU node. DaemonSet pods are node
/// plumbing and do not count.
fn uses_gpu(pod: &Pod, gpu_nodes: &BTreeSet<String>) -> bool {
    if pod.owner_references().iter().any(|o| o.kind == "DaemonSet") {
        return false;
    }
    let Some(spec) = &pod.spec else { return false };
    let containers = spec.containers.iter().chain(spec.init_containers.iter().flatten());
    let asks = containers.into_iter().any(|c| {
        c.resources.as_ref().is_some_and(|r| {
            r.requests.iter().chain(r.limits.iter()).any(|m| m.contains_key(GPU_RESOURCE))
        })
    });
    asks || spec.node_name.as_ref().is_some_and(|n| gpu_nodes.contains(n))
}

// Cluster edges, kept thin.

fn scaled_object_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk("keda.sh", "v1alpha1", "ScaledObject"), "scaledobjects")
}

async fn discover(kube: kube::Client) -> Result<Vec<Group>, ClusterError> {
    let servers = list_servers(kube.clone()).await?;
    let api: Api<DynamicObject> = Api::all_with(kube, &scaled_object_resource());
    let found = api.list(&ListParams::default().labels(L_GROUP)).await?;
    let mut gates = found.items.iter().map(parse_gate).collect::<Result<Vec<_>, _>>().map_err(ClusterError::Invalid)?;
    gates.sort_by_key(Gate::id);
    build_groups(&servers, gates).map_err(ClusterError::Invalid)
}

/// Set the pause annotation to `replicas`, or remove it (None) to hand the gate back to KEDA.
async fn patch_gate(kube: kube::Client, gate: &Gate, replicas: Option<&str>) -> Result<(), kube::Error> {
    let api: Api<DynamicObject> = Api::namespaced_with(kube, &gate.namespace, &scaled_object_resource());
    let value = replicas.map_or(serde_json::Value::Null, |r| serde_json::Value::String(r.to_string()));
    let annotations = serde_json::Map::from_iter([(PAUSE.to_string(), value)]);
    let body = serde_json::json!({ "metadata": { "annotations": annotations } });
    let params = PatchParams { field_manager: Some(FIELD_MANAGER.to_string()), ..PatchParams::default() };
    api.patch(&gate.name, &params, &Patch::Merge(body)).await?;
    Ok(())
}

async fn holders(kube: kube::Client, drain: Drain) -> Result<Vec<String>, kube::Error> {
    let pods = Api::<Pod>::all(kube.clone()).list(&ListParams::default()).await?.items;
    let live = pods.iter().filter(|p| is_live(p));
    Ok(match drain {
        Drain::Nfs => {
            let pvs = Api::<PersistentVolume>::all(kube).list(&ListParams::default()).await?.items;
            let claims: BTreeSet<String> = pvs
                .iter()
                .filter_map(|pv| {
                    let spec = pv.spec.as_ref()?;
                    let claim = spec.claim_ref.as_ref()?;
                    spec.nfs.as_ref()?;
                    Some(format!("{}/{}", claim.namespace.as_deref()?, claim.name.as_deref()?))
                })
                .collect();
            live.filter(|p| mounts_nfs(p, &claims)).map(describe).collect()
        }
        Drain::Gpu => {
            let nodes = Api::<Node>::all(kube).list(&ListParams::default().labels(GPU_NODE_LABEL)).await?.items;
            let nodes: BTreeSet<String> = nodes.iter().map(ResourceExt::name_any).collect();
            live.filter(|p| uses_gpu(p, &nodes)).map(describe).collect()
        }
    })
}

/// Drives the ScaledObjects of one group, keyed by group name, and powers the
/// group's server up or down around them. Exclusive handlers serialise per group.
pub struct KedaGroup {
    kube: kube::Client,
    drain_timeout: Duration,
}

impl KedaGroup {
    pub fn new(kube: kube::Client, drain_timeout: Duration) -> Self {
        Self { kube, drain_timeout }
    }

    async fn patch(&self, ctx: &ObjectContext<'_>, gate: &Gate, replicas: Option<&str>) -> Result<(), HandlerError> {
        let kube = self.kube.clone();
        ctx.run(|| async {
            patch_gate(kube, gate, replicas).await?;
            Ok(())
        })
        .name("patch_gate")
        .await?;
        Ok(())
    }

    /// Wait, bounded, for nothing to hold the server any more. The error! line is
    /// the hook for outside alerting; match on event="drain_timeout".
    async fn drain(&self, ctx: &ObjectContext<'_>, server: &Server, drain: Drain) -> Result<(), HandlerError> {
        let attempts = self.drain_timeout.as_secs() / DRAIN_POLL.as_secs();
        let mut held = Vec::new();
        for attempt in 0..=attempts {
            let kube = self.kube.clone();
            held = ctx.run(|| async { Ok(Json(holders(kube, drain).await?)) }).name("holders").await?.into_inner();
            if held.is_empty() {
                return Ok(());
            }
            if attempt < attempts {
                ctx.sleep(DRAIN_POLL).await?;
            }
        }
        tracing::error!(
            event = "drain_timeout",
            server = %server.name,
            timeout_secs = self.drain_timeout.as_secs(),
            holders = held.len(),
            "{} is still held after the drain wait: {}",
            server.name,
            held.join("; "),
        );
        Err(TerminalError::new_with_code(
            504,
            format!(
                "timed out after {:?}; still holding {}:\n{}\n(pods not owned by a gated ScaledObject must be stopped by hand)",
                self.drain_timeout,
                server.name,
                held.join("\n"),
            ),
        )
        .into())
    }
}

#[derive(Serialize, Deserialize)]
pub struct ModeRequest {
    /// off, auto, or a role the group has (see KedaCatalog/groups).
    pub mode: String,
}

#[derive(Serialize, Deserialize)]
pub struct ModeOutcome {
    group: String,
    server: Option<String>,
    paused: Vec<String>,
    held: Vec<String>,
    unpaused: Vec<String>,
}

#[restate_sdk::object]
impl KedaGroup {
    /// Pause, then power the server and drain it as the mode requires, then hold
    /// and unpause. Off pauses everything, waits for the holders to clear, and
    /// powers the server off; any other mode powers it on first and only then lets
    /// workloads start, so they never schedule onto a host that is not up.
    ///
    /// A failure after the first change leaves the earlier changes in place; they
    /// are idempotent, so the caller can fix the cause and send the same request.
    #[handler]
    async fn set_mode(&self, ctx: ObjectContext<'_>, Json(req): Json<ModeRequest>) -> Result<Json<ModeOutcome>, HandlerError> {
        let name = ctx.key().to_string();
        let mode = Mode::parse(&req.mode);
        let kube = self.kube.clone();
        let groups = ctx.run(|| async { Ok(Json(discover(kube).await?)) }).name("discover").await?.into_inner();
        let group = groups
            .iter()
            .find(|g| g.name == name)
            .ok_or_else(|| TerminalError::new_with_code(404, format!("no ScaledObjects labelled {L_GROUP}={name}")))?;
        let plan = plan_mode(group, &mode, &groups)?;

        for gate in &plan.pause {
            self.patch(&ctx, gate, Some("0")).await?;
        }
        if let Some(server) = &group.server {
            let power = ctx.object_client::<PowerObjectClient>(server.name.clone());
            match (&mode, server.drain) {
                (Mode::Off, Some(drain)) => {
                    self.drain(&ctx, server, drain).await?;
                    power.power_off().call().await?;
                }
                _ => {
                    power.power_on().call().await?;
                }
            }
        }
        for gate in &plan.hold {
            self.patch(&ctx, gate, Some("1")).await?;
        }
        for gate in &plan.unpause {
            self.patch(&ctx, gate, None).await?;
        }

        let ids = |gates: &[Gate]| gates.iter().map(Gate::id).collect();
        Ok(Json(ModeOutcome {
            group: name,
            server: group.server.as_ref().map(|s| s.name.clone()),
            paused: ids(&plan.pause),
            held: ids(&plan.hold),
            unpaused: ids(&plan.unpause),
        }))
    }
}

/// Lists the groups and the modes each accepts, so callers need not know them.
pub struct KedaCatalog {
    kube: kube::Client,
}

impl KedaCatalog {
    pub fn new(kube: kube::Client) -> Self {
        Self { kube }
    }
}

#[restate_sdk::service]
impl KedaCatalog {
    #[handler]
    async fn groups(&self, ctx: Context<'_>) -> Result<Json<Vec<GroupView>>, HandlerError> {
        let kube = self.kube.clone();
        let groups = ctx.run(|| async { Ok(Json(discover(kube).await?)) }).name("discover").await?.into_inner();
        Ok(Json(groups.iter().map(GroupView::from).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::Server;
    use serde_json::json;

    fn gate(name: &str, group: &str, role: Option<&str>, paused: Option<&str>) -> Gate {
        Gate {
            namespace: "ns".into(),
            name: name.into(),
            group: group.into(),
            role: role.map(str::to_string),
            paused: paused.map(str::to_string),
            flux_owned: false,
        }
    }

    fn server(name: &str, groups: &[&str], exclusive: bool) -> Server {
        Server {
            name: name.into(),
            host: format!("{name}.kaffi.internal"),
            port: 22,
            mac: [1, 2, 3, 4, 5, 6],
            wol_interface: "lan".into(),
            shutdown_user: Some("poweroff".into()),
            host_key: Some("SHA256:abc".into()),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            drain: Some(Drain::Gpu),
            exclusive_roles: exclusive,
        }
    }

    fn gpu_group() -> Group {
        let servers = [server("cuda-central", &["gpu"], true)];
        let gates = vec![
            gate("client", "gpu", None, None),
            gate("llm", "gpu", Some("llm"), None),
            gate("ocr", "gpu", Some("ocr"), None),
        ];
        build_groups(&servers, gates).unwrap().remove(0)
    }

    fn ids(gates: &[Gate]) -> Vec<&str> {
        gates.iter().map(|g| g.name.as_str()).collect()
    }

    #[test]
    fn modes_parse() {
        assert_eq!(Mode::parse(" off "), Mode::Off);
        assert_eq!(Mode::parse("auto"), Mode::Auto);
        assert_eq!(Mode::parse("on"), Mode::Auto);
        assert_eq!(Mode::parse("ocr"), Mode::Role("ocr".into()));
    }

    #[test]
    fn roles_and_servers_are_discovered() {
        let g = gpu_group();
        assert_eq!(g.name, "gpu");
        assert_eq!(g.server.as_ref().unwrap().name, "cuda-central");
        assert_eq!(g.roles.iter().collect::<Vec<_>>(), ["llm", "ocr"]);
        let view = GroupView::from(&g);
        assert_eq!(view.modes, ["off", "auto", "llm", "ocr"]);
        assert!(view.exclusive_roles);
    }

    #[test]
    fn group_without_a_server_is_allowed() {
        let groups = build_groups(&[], vec![gate("a", "idle", None, None)]).unwrap();
        assert!(groups[0].server.is_none());
    }

    #[test]
    fn two_servers_for_one_group_is_an_error() {
        let servers = [server("a", &["gpu"], false), server("b", &["gpu"], false)];
        assert!(build_groups(&servers, vec![gate("x", "gpu", None, None)]).is_err());
    }

    #[test]
    fn wanted_replicas_by_mode() {
        let client = gate("c", "gpu", None, None);
        let llm = gate("l", "gpu", Some("llm"), None);
        let ocr = gate("o", "gpu", Some("ocr"), None);
        let role = Mode::Role("llm".into());
        assert_eq!(wanted_replicas(&Mode::Off, true, &client), Some("0"));
        assert_eq!(wanted_replicas(&Mode::Auto, true, &llm), None);
        assert_eq!(wanted_replicas(&role, true, &client), None, "role-less gates follow the server");
        assert_eq!(wanted_replicas(&role, true, &llm), Some("1"));
        assert_eq!(wanted_replicas(&role, true, &ocr), Some("0"), "exclusive roles are paused");
        assert_eq!(wanted_replicas(&role, false, &ocr), None, "non-exclusive roles are left to KEDA");
    }

    #[test]
    fn role_mode_pauses_holds_and_leaves_clients() {
        let g = gpu_group();
        let plan = plan_mode(&g, &Mode::Role("llm".into()), &[g.clone()]).unwrap();
        assert_eq!(ids(&plan.pause), ["ocr"]);
        assert_eq!(ids(&plan.hold), ["llm"]);
        assert!(plan.unpause.is_empty());
    }

    #[test]
    fn auto_unpauses_only_what_is_paused() {
        let mut g = gpu_group();
        g.gates[0].paused = Some("0".into());
        g.gates[1].paused = Some("1".into());
        let plan = plan_mode(&g, &Mode::Auto, &[g.clone()]).unwrap();
        assert_eq!(ids(&plan.unpause), ["client", "llm"]);
        assert!(plan.pause.is_empty() && plan.hold.is_empty());
    }

    #[test]
    fn off_when_already_paused_changes_nothing() {
        let mut g = gpu_group();
        for gate in &mut g.gates {
            gate.paused = Some("0".into());
        }
        assert_eq!(plan_mode(&g, &Mode::Off, &[g.clone()]).unwrap(), Plan::default());
    }

    #[test]
    fn unknown_or_reserved_roles_are_refused() {
        let g = gpu_group();
        let err = plan_mode(&g, &Mode::Role("asr".into()), &[g.clone()]).unwrap_err();
        assert_eq!(err.code, 400);
        assert!(err.message.contains("off, auto, llm, ocr"));

        let clash = build_groups(&[], vec![gate("x", "idle", Some("off"), None)]).unwrap().remove(0);
        assert_eq!(plan_mode(&clash, &Mode::Auto, &[clash.clone()]).unwrap_err().code, 422);
    }

    #[test]
    fn flux_owned_annotation_is_refused() {
        let mut g = gpu_group();
        g.gates[0].paused = Some("0".into());
        g.gates[0].flux_owned = true;
        assert_eq!(plan_mode(&g, &Mode::Auto, &[g.clone()]).unwrap_err().code, 409);
        // Already what we want: nothing to change, so nothing to refuse.
        assert!(plan_mode(&g, &Mode::Off, &[g.clone()]).is_ok());
    }

    #[test]
    fn off_needs_drain_and_shutdown_fields_before_anything_changes() {
        let mut g = gpu_group();
        g.server.as_mut().unwrap().drain = None;
        assert_eq!(plan_mode(&g, &Mode::Off, &[g.clone()]).unwrap_err().code, 412);

        let mut g = gpu_group();
        g.server.as_mut().unwrap().host_key = None;
        assert_eq!(plan_mode(&g, &Mode::Off, &[g.clone()]).unwrap_err().code, 412);
        assert!(plan_mode(&g, &Mode::Auto, &[g.clone()]).is_ok(), "powering on needs neither");
    }

    #[test]
    fn off_is_refused_while_another_group_on_the_server_is_live() {
        let servers = [server("big", &["a", "b"], false)];
        let gates = vec![gate("x", "a", None, None), gate("y", "b", None, None)];
        let groups = build_groups(&servers, gates).unwrap();
        let err = plan_mode(&groups[0], &Mode::Off, &groups).unwrap_err();
        assert_eq!(err.code, 409);

        let mut groups = groups;
        groups[1].gates[0].paused = Some("0".into());
        assert!(plan_mode(&groups[0], &Mode::Off, &groups).is_ok());
    }

    #[test]
    fn flux_ownership_is_read_from_managed_fields() {
        let entry = |manager: &str, annotated: bool| -> ManagedFieldsEntry {
            let fields = if annotated {
                json!({"f:metadata": {"f:annotations": {format!("f:{PAUSE}"): {}}}})
            } else {
                json!({"f:metadata": {"f:labels": {}}})
            };
            serde_json::from_value(json!({"manager": manager, "operation": "Apply", "fieldsV1": fields})).unwrap()
        };
        assert!(owned_by_flux(&[entry("flowmanager", true), entry(FLUX_MANAGER, true)]));
        assert!(!owned_by_flux(&[entry("flowmanager", true)]));
        assert!(!owned_by_flux(&[entry(FLUX_MANAGER, false)]));
    }

    #[test]
    fn gate_is_parsed_from_a_scaled_object() {
        let obj: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "keda.sh/v1alpha1",
            "kind": "ScaledObject",
            "metadata": {
                "name": "ollama",
                "namespace": "ollama",
                "labels": { L_GROUP: "gpu", L_ROLE: "llm" },
                "annotations": { PAUSE: "0" },
            },
        }))
        .unwrap();
        let g = parse_gate(&obj).unwrap();
        assert_eq!((g.group.as_str(), g.role.as_deref(), g.paused.as_deref()), ("gpu", Some("llm"), Some("0")));
        assert_eq!(g.id(), "ollama/ollama");
        assert!(!g.flux_owned);
    }

    fn pod(value: serde_json::Value) -> Pod {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn gpu_holders() {
        let nodes: BTreeSet<String> = ["cuda-central".to_string()].into();
        let wants = pod(json!({"metadata": {"name": "a", "namespace": "n"}, "spec": {"containers": [
            {"name": "c", "resources": {"limits": {GPU_RESOURCE: "1"}}}]}}));
        let on_node = pod(json!({"metadata": {"name": "b", "namespace": "n"},
            "spec": {"nodeName": "cuda-central", "containers": [{"name": "c"}]}}));
        let plain = pod(json!({"metadata": {"name": "c", "namespace": "n"},
            "spec": {"nodeName": "other", "containers": [{"name": "c"}]}}));
        let daemon = pod(json!({"metadata": {"name": "d", "namespace": "n",
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "x", "uid": "u"}]},
            "spec": {"nodeName": "cuda-central", "containers": [{"name": "c"}]}}));
        assert!(uses_gpu(&wants, &nodes));
        assert!(uses_gpu(&on_node, &nodes));
        assert!(!uses_gpu(&plain, &nodes));
        assert!(!uses_gpu(&daemon, &nodes));
    }

    #[test]
    fn nfs_holders() {
        let claims: BTreeSet<String> = ["n/data".to_string()].into();
        let claimed = pod(json!({"metadata": {"name": "a", "namespace": "n"}, "spec": {"containers": [],
            "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "data"}}]}}));
        let other_ns = pod(json!({"metadata": {"name": "b", "namespace": "m"}, "spec": {"containers": [],
            "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "data"}}]}}));
        let inline = pod(json!({"metadata": {"name": "c", "namespace": "m"}, "spec": {"containers": [],
            "volumes": [{"name": "v", "nfs": {"server": "s", "path": "/p"}}]}}));
        assert!(mounts_nfs(&claimed, &claims));
        assert!(!mounts_nfs(&other_ns, &claims));
        assert!(mounts_nfs(&inline, &claims));
    }

    #[test]
    fn finished_pods_do_not_hold() {
        let done = pod(json!({"metadata": {"name": "a", "namespace": "n"}, "status": {"phase": "Succeeded"}}));
        let running = pod(json!({"metadata": {"name": "a", "namespace": "n"}, "status": {"phase": "Running"}}));
        assert!(!is_live(&done));
        assert!(is_live(&running));
        assert_eq!(describe(&running), "n/a Running");
    }
}
