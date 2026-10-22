use restate_sdk::prelude::*;


pub struct PowerWorkflow;

type MacAddress = String;

#[derive(serde::Serialize, serde::Deserialize)]
struct PowerTarget {
    desired_state: bool,
    target: MacAddress,
}

#[restate_sdk::workflow]
impl PowerWorkflow {
    #[handler]
    async fn run(&self, _ctx: WorkflowContext<'_>, Json(_target): Json<PowerTarget>) -> Result<bool, HandlerError> {
        Ok(true)
    }

    // check_state(target_port)
    // power_on(mac_address)
    // power_off(ip_address, ssh_shutdown_key)
}