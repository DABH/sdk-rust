use temporalio_sdk::interceptors::{
    ExecuteInput, Next, WorkflowExecuteOutput, WorkflowInboundInterceptor,
};

struct BadInterceptor;

impl WorkflowInboundInterceptor for BadInterceptor {
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        Box::pin(async move {
            futures::future::ready(()).await;
            next.run(input).await
        })
    }
}

fn main() {}
