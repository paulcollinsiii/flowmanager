use restate_sdk::prelude::*;

type KedaRole = String;

const L_SERVER = "power.kaffi.internal/server";
const L_ROLE = "power.kaffi.internal/gpu-role";
const L_GROUP = "power.kaffi.internal/group";

struct Server {
    name: String,
}
struct Pod {
    namespace: String,
    name: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DesiredKedaState {
    desired_state: bool,
    target: KedaRole,
}

async fn discover_servers() -> Result<Vec::Server, Box<dyn std::error::Error>> {
    let mut svrs: Vec<Server> = Vec::new();
    svrs
}

async fn nfs_holders() -> Result<Vec::Pod, Box<dyn std::error::Error>> {
    let mut pods: Vec<Pod> = Vec::new();
    pods
}

#[restate_sdk::workflow]
impl KedaWorkflow {
    #[handler]
    async fn run(&self, _cts: WorkflowContext<'_>, Json(_desired_state): Json<DesiredKedaState>) -> Result<bool, HandlerError> {
        // Get the list of servers from Kubernetes
        // Turning things on?
            // Trigger power workflow to turn on svr
        // What set of pods are actually desired
        // Set pod values
        // Turning things off?
            // Trigger power workflow to turn off svr
        Ok(true)
    }
}