use temporalio_sdk::interceptors::workflows::{
    ExecuteInput, Next, ExecuteOutput, WorkflowInboundInterceptor,
};

struct BadInterceptor;

impl WorkflowInboundInterceptor for BadInterceptor {
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, ExecuteOutput>,
    ) -> ExecuteOutput {
        Box::pin(async move {
            futures::future::ready(()).await;
            next.run(input).await
        })
    }
}

fn main() {}
