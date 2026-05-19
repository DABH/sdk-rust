use temporalio_sdk::interceptors::{
    ExecuteInput, Next, WorkflowExecuteOutput, WorkflowInboundInterceptor,
};

struct GoodInterceptor;

impl WorkflowInboundInterceptor for GoodInterceptor {
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        // Forward to `next` synchronously, then wrap the returned future to observe completion.
        let inner = next.run(input);
        Box::pin(async move {
            let result = inner.await;
            let _observed = result.is_ok();
            result
        })
    }
}

fn main() {}
