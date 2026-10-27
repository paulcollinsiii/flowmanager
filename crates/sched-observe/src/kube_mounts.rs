//! Which pods mount an NFS host, read from the Kubernetes API: the veto before a managed
//! NFS host is powered off. Read-only: it lists PersistentVolumes, PersistentVolumeClaims
//! and pods, nothing else.
//!
//! A pod mounts a host when it has not finished (anything but `Succeeded` or `Failed`; a
//! terminating pod still holds its mounts) and one of its volumes is an NFS volume of one
//! of the host's configured servers, directly (`nfs`, or an inline NFS CSI volume) or through
//! a PersistentVolumeClaim bound to such a PersistentVolume (`spec.nfs`, or an NFS CSI
//! volume's `server` attribute). The servers come from the config (`nfs_servers`), never from
//! the code.
//!
//! Unknown is not zero. A list that fails or times out is a failed read (the reading goes
//! stale), and so is a pod that refers to a claim, or a claim to a volume, that the lists
//! do not show (the lists are taken one after the other, so such a pod may mount something
//! this reading cannot see). The one thing that is not counted: a claim not bound to a
//! volume yet, which cannot be mounted.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim, Pod, Volume};
use kube::api::ListParams;
use kube::{Api, ResourceExt};
use sched_model::{KubeWatchParams, ObservedValue, ResourceId};

/// How long the three lists may take in all before the reading is reported as failed.
pub const READ_TIMEOUT: Duration = Duration::from_secs(20);

/// The server of an NFS CSI volume, lowercased, from its driver name and attributes.
fn csi_server(
    driver: &str,
    attributes: Option<&std::collections::BTreeMap<String, String>>,
) -> Option<String> {
    // The NFS CSI driver (csi-driver-nfs) names its server in the `server` attribute.
    if !driver.to_ascii_lowercase().contains("nfs") {
        return None;
    }
    attributes?.get("server").map(|s| s.to_ascii_lowercase())
}

/// The NFS server a PersistentVolume is on, lowercased, if it is an NFS volume.
pub fn pv_server(pv: &PersistentVolume) -> Option<String> {
    let spec = pv.spec.as_ref()?;
    if let Some(nfs) = &spec.nfs {
        return Some(nfs.server.to_ascii_lowercase());
    }
    let csi = spec.csi.as_ref()?;
    csi_server(&csi.driver, csi.volume_attributes.as_ref())
}

/// What the lists show: PersistentVolume name to its NFS server (absent for a volume that
/// is not NFS), and claim `(namespace, name)` to the volume it is bound to (`None` while
/// unbound).
#[derive(Debug, Default)]
pub struct Cluster {
    pub volumes: HashMap<String, Option<String>>,
    pub claims: HashMap<(String, String), Option<String>>,
}

impl Cluster {
    pub fn new(pvs: &[PersistentVolume], pvcs: &[PersistentVolumeClaim]) -> Cluster {
        Cluster {
            volumes: pvs
                .iter()
                .map(|pv| (pv.name_any(), pv_server(pv)))
                .collect(),
            claims: pvcs
                .iter()
                .map(|c| {
                    (
                        (c.namespace().unwrap_or_default(), c.name_any()),
                        c.spec.as_ref().and_then(|s| s.volume_name.clone()),
                    )
                })
                .collect(),
        }
    }

    /// The NFS server behind claim `name` of `namespace`, lowercased: `Ok(None)` if it is
    /// not NFS or not bound yet, an error if the lists do not show the claim or its volume.
    fn claim_server(&self, namespace: &str, name: &str) -> Result<Option<String>, String> {
        let bound = self
            .claims
            .get(&(namespace.to_string(), name.to_string()))
            .ok_or_else(|| format!("claim {namespace}/{name} is not in the claim list"))?;
        let Some(volume) = bound else {
            return Ok(None);
        };
        match self.volumes.get(volume) {
            Some(server) => Ok(server.clone()),
            None => Err(format!(
                "claim {namespace}/{name} is bound to volume {volume}, which is not in the volume list"
            )),
        }
    }
}

/// Whether the pod still holds its volumes: anything but finished.
fn pod_active(pod: &Pod) -> bool {
    let phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("");
    !matches!(phase, "Succeeded" | "Failed")
}

/// The NFS servers `pod` mounts, lowercased; an error if a claim it uses cannot be
/// resolved.
pub fn pod_servers(pod: &Pod, cluster: &Cluster) -> Result<Vec<String>, String> {
    let namespace = pod.namespace().unwrap_or_default();
    let mut out = Vec::new();
    let volumes: &[Volume] = pod
        .spec
        .as_ref()
        .and_then(|s| s.volumes.as_deref())
        .unwrap_or_default();
    for v in volumes {
        if let Some(nfs) = &v.nfs {
            out.push(nfs.server.to_ascii_lowercase());
        }
        if let Some(csi) = &v.csi {
            out.extend(csi_server(&csi.driver, csi.volume_attributes.as_ref()));
        }
        let claim = match (&v.persistent_volume_claim, &v.ephemeral) {
            (Some(c), _) => Some(c.claim_name.clone()),
            // A generic ephemeral volume's claim is named after the pod and the volume.
            (None, Some(_)) => Some(format!("{}-{}", pod.name_any(), v.name)),
            (None, None) => None,
        };
        if let Some(claim) = claim
            && let Some(server) = cluster
                .claim_server(&namespace, &claim)
                .map_err(|e| format!("pod {namespace}/{}: {e}", pod.name_any()))?
        {
            out.push(server);
        }
    }
    Ok(out)
}

/// One `ResourceMounts` per resource of `nfs_servers`: the number of active pods with a
/// volume on one of its servers (zero if none: the lists are complete, so absence is a
/// fact). An error if any active pod's volumes cannot be resolved.
pub fn mount_values(
    nfs_servers: &BTreeMap<ResourceId, Vec<String>>,
    pods: &[Pod],
    cluster: &Cluster,
) -> Result<Vec<ObservedValue>, String> {
    let mut counts: BTreeMap<&ResourceId, u32> = nfs_servers.keys().map(|r| (r, 0)).collect();
    for pod in pods.iter().filter(|p| pod_active(p)) {
        let servers = pod_servers(pod, cluster)?;
        for (resource, wanted) in nfs_servers {
            let hit = servers
                .iter()
                .any(|s| wanted.iter().any(|w| w.eq_ignore_ascii_case(s)));
            if hit && let Some(n) = counts.get_mut(resource) {
                *n += 1;
            }
        }
    }
    Ok(counts
        .into_iter()
        .map(|(resource, pods)| ObservedValue::ResourceMounts {
            resource: resource.clone(),
            pods,
        })
        .collect())
}

/// Lists `K` in the namespaces of `params` (every namespace if none are listed).
async fn list_all<K>(
    client: &kube::Client,
    namespaces: &[String],
    what: &str,
) -> Result<Vec<K>, String>
where
    K: kube::Resource<DynamicType = (), Scope = kube::core::NamespaceResourceScope>
        + Clone
        + serde::de::DeserializeOwned
        + std::fmt::Debug,
{
    let lp = ListParams::default();
    let mut out = Vec::new();
    if namespaces.is_empty() {
        let api: Api<K> = Api::all(client.clone());
        out.extend(
            api.list(&lp)
                .await
                .map_err(|e| format!("listing {what}: {e}"))?
                .items,
        );
    }
    for ns in namespaces {
        let api: Api<K> = Api::namespaced(client.clone(), ns);
        out.extend(
            api.list(&lp)
                .await
                .map_err(|e| format!("listing {what} in namespace {ns}: {e}"))?
                .items,
        );
    }
    Ok(out)
}

async fn read(
    client: &kube::Client,
    params: &KubeWatchParams,
) -> Result<Vec<ObservedValue>, String> {
    // Volumes and claims first, pods last: whatever a listed pod refers to was created
    // before the pod list, so a claim missing from the claim list is a race to retry.
    let pvs = Api::<PersistentVolume>::all(client.clone())
        .list(&ListParams::default())
        .await
        .map_err(|e| format!("listing PersistentVolumes: {e}"))?
        .items;
    let pvcs: Vec<PersistentVolumeClaim> =
        list_all(client, &params.namespaces, "PersistentVolumeClaims").await?;
    let pods: Vec<Pod> = list_all(client, &params.namespaces, "pods").await?;
    mount_values(&params.nfs_servers, &pods, &Cluster::new(&pvs, &pvcs))
}

/// The mount readings, or why they could not be read. Bounded by [`READ_TIMEOUT`].
pub async fn read_mounts(
    client: &kube::Client,
    params: &KubeWatchParams,
) -> Result<Vec<ObservedValue>, String> {
    tokio::time::timeout(READ_TIMEOUT, read(client, params))
        .await
        .map_err(|_| {
            format!(
                "reading mounts took longer than {}s",
                READ_TIMEOUT.as_secs()
            )
        })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        CSIPersistentVolumeSource, CSIVolumeSource, EphemeralVolumeSource, NFSVolumeSource,
        PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PersistentVolumeSpec,
        PodSpec, PodStatus,
    };

    fn nfs_volume(name: &str, server: &str) -> Volume {
        Volume {
            name: name.into(),
            nfs: Some(NFSVolumeSource {
                server: server.into(),
                path: "/export".into(),
                read_only: None,
            }),
            ..Default::default()
        }
    }

    fn claim_volume(name: &str, claim: &str) -> Volume {
        Volume {
            name: name.into(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: claim.into(),
                read_only: None,
            }),
            ..Default::default()
        }
    }

    fn pod(name: &str, ns: &str, phase: &str, volumes: Vec<Volume>) -> Pod {
        let mut p = Pod::default();
        p.metadata.name = Some(name.into());
        p.metadata.namespace = Some(ns.into());
        p.spec = Some(PodSpec {
            volumes: Some(volumes),
            ..Default::default()
        });
        p.status = Some(PodStatus {
            phase: Some(phase.into()),
            ..Default::default()
        });
        p
    }

    fn pv_nfs(name: &str, server: &str) -> PersistentVolume {
        let mut pv = PersistentVolume::default();
        pv.metadata.name = Some(name.into());
        pv.spec = Some(PersistentVolumeSpec {
            nfs: Some(NFSVolumeSource {
                server: server.into(),
                path: "/export".into(),
                read_only: None,
            }),
            ..Default::default()
        });
        pv
    }

    fn pv_local(name: &str) -> PersistentVolume {
        let mut pv = PersistentVolume::default();
        pv.metadata.name = Some(name.into());
        pv.spec = Some(PersistentVolumeSpec::default());
        pv
    }

    fn pvc(name: &str, ns: &str, volume: Option<&str>) -> PersistentVolumeClaim {
        let mut c = PersistentVolumeClaim::default();
        c.metadata.name = Some(name.into());
        c.metadata.namespace = Some(ns.into());
        c.spec = Some(PersistentVolumeClaimSpec {
            volume_name: volume.map(str::to_string),
            ..Default::default()
        });
        c
    }

    fn servers() -> BTreeMap<ResourceId, Vec<String>> {
        BTreeMap::from([(
            ResourceId::new("basestar-nfs"),
            vec!["basestar".to_string(), "192.168.1.5".to_string()],
        )])
    }

    fn count(pods: &[Pod], cluster: &Cluster) -> Result<u32, String> {
        let v = mount_values(&servers(), pods, cluster)?;
        assert_eq!(v.len(), 1);
        match &v[0] {
            ObservedValue::ResourceMounts { resource, pods } => {
                assert_eq!(resource.as_str(), "basestar-nfs");
                Ok(*pods)
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_direct_nfs_volume_of_a_configured_server_counts_by_name_or_address_ignoring_case() {
        let c = Cluster::default();
        let pods = [
            pod("a", "docs", "Running", vec![nfs_volume("v", "BaseStar")]),
            pod("b", "docs", "Pending", vec![nfs_volume("v", "192.168.1.5")]),
            pod("c", "docs", "Running", vec![nfs_volume("v", "other")]),
            pod("d", "docs", "Running", vec![]),
        ];
        assert_eq!(count(&pods, &c), Ok(2));
    }

    #[test]
    fn a_configured_server_written_in_mixed_case_still_matches_a_pods_server() {
        // `pod_servers` lowercases what the pod says; the configured name is compared
        // ignoring case too, so a config that spells the host in capitals matches.
        let configured = BTreeMap::from([(
            ResourceId::new("basestar-nfs"),
            vec!["BaseStar.Local".to_string()],
        )]);
        let pods = [
            pod(
                "a",
                "docs",
                "Running",
                vec![nfs_volume("v", "basestar.local")],
            ),
            pod(
                "b",
                "docs",
                "Running",
                vec![nfs_volume("v", "BASESTAR.LOCAL")],
            ),
            pod(
                "c",
                "docs",
                "Running",
                vec![nfs_volume("v", "basestar.other")],
            ),
        ];
        let v = mount_values(&configured, &pods, &Cluster::default()).unwrap();
        assert_eq!(
            v,
            vec![ObservedValue::ResourceMounts {
                resource: ResourceId::new("basestar-nfs"),
                pods: 2
            }]
        );
    }

    #[test]
    fn a_claim_bound_to_an_nfs_volume_counts_and_one_bound_elsewhere_does_not() {
        let c = Cluster::new(
            &[pv_nfs("pv-docs", "basestar"), pv_local("pv-scratch")],
            &[
                pvc("docs", "docs", Some("pv-docs")),
                pvc("scratch", "docs", Some("pv-scratch")),
            ],
        );
        let pods = [
            pod("a", "docs", "Running", vec![claim_volume("v", "docs")]),
            pod("b", "docs", "Running", vec![claim_volume("v", "scratch")]),
        ];
        assert_eq!(count(&pods, &c), Ok(1));
    }

    #[test]
    fn an_nfs_csi_volume_counts_through_its_server_attribute_and_other_drivers_do_not() {
        let mut csi = PersistentVolume::default();
        csi.metadata.name = Some("pv-csi".into());
        csi.spec = Some(PersistentVolumeSpec {
            csi: Some(CSIPersistentVolumeSource {
                driver: "nfs.csi.k8s.io".into(),
                volume_attributes: Some(BTreeMap::from([("server".into(), "basestar".into())])),
                ..Default::default()
            }),
            ..Default::default()
        });
        let mut other = PersistentVolume::default();
        other.metadata.name = Some("pv-ebs".into());
        other.spec = Some(PersistentVolumeSpec {
            csi: Some(CSIPersistentVolumeSource {
                driver: "ebs.csi.aws.com".into(),
                volume_attributes: Some(BTreeMap::from([("server".into(), "basestar".into())])),
                ..Default::default()
            }),
            ..Default::default()
        });
        let c = Cluster::new(
            &[csi, other],
            &[
                pvc("a", "docs", Some("pv-csi")),
                pvc("b", "docs", Some("pv-ebs")),
            ],
        );
        let inline = Volume {
            name: "inline".into(),
            csi: Some(CSIVolumeSource {
                driver: "nfs.csi.k8s.io".into(),
                volume_attributes: Some(BTreeMap::from([("server".into(), "basestar".into())])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pods = [
            pod("a", "docs", "Running", vec![claim_volume("v", "a")]),
            pod("b", "docs", "Running", vec![claim_volume("v", "b")]),
            pod("c", "docs", "Running", vec![inline]),
        ];
        assert_eq!(count(&pods, &c), Ok(2));
    }

    #[test]
    fn a_generic_ephemeral_volume_is_resolved_through_its_pod_named_claim() {
        let c = Cluster::new(
            &[pv_nfs("pv-e", "basestar")],
            &[pvc("job-1-scratch", "docs", Some("pv-e"))],
        );
        let eph = Volume {
            name: "scratch".into(),
            ephemeral: Some(EphemeralVolumeSource::default()),
            ..Default::default()
        };
        assert_eq!(
            count(&[pod("job-1", "docs", "Running", vec![eph])], &c),
            Ok(1)
        );
    }

    #[test]
    fn finished_pods_do_not_count_but_terminating_and_pending_ones_do() {
        let c = Cluster::default();
        let v = || vec![nfs_volume("v", "basestar")];
        let pods = [
            pod("done", "docs", "Succeeded", v()),
            pod("failed", "docs", "Failed", v()),
            pod("pending", "docs", "Pending", v()),
            pod("unknown", "docs", "Unknown", v()),
        ];
        assert_eq!(count(&pods, &c), Ok(2));
        let mut no_status = pod("fresh", "docs", "Running", v());
        no_status.status = None;
        assert_eq!(count(&[no_status], &c), Ok(1));
    }

    #[test]
    fn no_pods_is_zero_for_each_configured_resource() {
        assert_eq!(count(&[], &Cluster::default()), Ok(0));
    }

    #[test]
    fn a_claim_or_volume_the_lists_do_not_show_is_a_failed_read_not_zero() {
        let pods = [pod("a", "docs", "Running", vec![claim_volume("v", "docs")])];
        let e = count(&pods, &Cluster::default()).unwrap_err();
        assert!(
            e.contains("claim docs/docs is not in the claim list"),
            "{e}"
        );
        let c = Cluster::new(&[], &[pvc("docs", "docs", Some("pv-gone"))]);
        let e = count(&pods, &c).unwrap_err();
        assert!(
            e.contains("volume pv-gone, which is not in the volume list"),
            "{e}"
        );
        // A finished pod's volumes are not resolved at all.
        let done = [pod(
            "a",
            "docs",
            "Succeeded",
            vec![claim_volume("v", "docs")],
        )];
        assert_eq!(count(&done, &Cluster::default()), Ok(0));
    }

    #[test]
    fn a_claim_not_bound_yet_cannot_be_mounted_and_is_not_counted() {
        let c = Cluster::new(&[], &[pvc("docs", "docs", None)]);
        let pods = [pod("a", "docs", "Pending", vec![claim_volume("v", "docs")])];
        assert_eq!(count(&pods, &c), Ok(0));
    }

    #[test]
    fn claims_are_resolved_in_the_pods_own_namespace() {
        let c = Cluster::new(
            &[pv_nfs("pv-docs", "basestar")],
            &[
                pvc("data", "docs", Some("pv-docs")),
                pvc("data", "media", None),
            ],
        );
        let pods = [
            pod("a", "docs", "Running", vec![claim_volume("v", "data")]),
            pod("b", "media", "Running", vec![claim_volume("v", "data")]),
        ];
        assert_eq!(count(&pods, &c), Ok(1));
    }
}
