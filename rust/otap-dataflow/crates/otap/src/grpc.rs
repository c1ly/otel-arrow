use grpc_stubs::proto::experimental::arrow::v1::{
    arrow_logs_service_server::ArrowLogsService,
    arrow_traces_service_server::ArrowTracesService,
    arrow_metrics_service_server::ArrowMetricsService,
    BatchArrowRecords, BatchStatus

};
use tonic::{Request, Response, Status};
use otap_df_engine::receiver::{EffectHandler, SendableMode};
use otap_df_channel::mpsc;
use std::pin::Pin;

use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

/// Expose the OTLP gRPC services.
/// See the build.rs file for more information.
pub mod grpc_stubs {
    #[path = ""]
    pub mod proto {
        #[path = ""]
        pub mod experimental {
            #[path = ""]
            pub mod arrow {
                #[allow(unused_qualifications)]
                #[allow(unused_results)]
                #[allow(clippy::enum_variant_names)]
                #[allow(rustdoc::invalid_html_tags)]
                #[path = "opentelemetry.proto.experimental.arrow.v1.rs"]
                pub mod v1;
            }
        }
    }
}


pub struct ArrowLogsServiceImpl {
    effect_handler: EffectHandler<BatchArrowRecords, SendableMode>,
}

impl ArrowLogsServiceImpl {
    pub fn new(effect_handler: EffectHandler<BatchArrowRecords, SendableMode>) -> Self {
        Self { effect_handler }
    }
}
pub struct ArrowMetricsServiceImpl {
    effect_handler: EffectHandler<BatchArrowRecords, SendableMode>,
}

impl ArrowMetricsServiceImpl {
    pub fn new(effect_handler: EffectHandler<BatchArrowRecords, SendableMode>) -> Self {
        Self { effect_handler }
    }
}
pub struct ArrowTraceServiceImpl {
    effect_handler: EffectHandler<BatchArrowRecords, SendableMode>,
}

impl ArrowTraceServiceImpl {
    pub fn new(effect_handler: EffectHandler<BatchArrowRecords, SendableMode>) -> Self {
        Self { effect_handler }
    }
}

#[tonic::async_trait]
impl ArrowLogsService for ArrowLogsServiceImpl {
    type ArrowLogsStream = Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;
    async fn arrow_logs(
        &self,
        request: Request<tonic::Streaming<super::BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowLogsStream>, tonic::Status> {

        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100); 
        let effect_handler_clone = self.effect_handler.clone();


        effect_handler_clone.send_message(OTAPRequest::Logs(request.into_inner())).await;
        
        // get channels tx,rx 
        let output = ReceiverStream::new(rx);

        // write to the channel 
        tokio::spawn_local(async move {
            while let Some(data) = stream.next().await {
                let data = data?;
                let effect_handler_clone.send_message(data);

                tx.send(Ok(BatchStatus {
                    batch_id: batch.batch_id,
                    status_code: status_result.0 as i32,
                    status_message: status_result.1,
                }))
                .await
                .map_err(|_| ())
            }
        });


        Ok(Response::new(Box::pin(output)
        as Self::ArrowLogsStream))
    }
}


#[tonic::async_trait]
impl ArrowMetricsService for OTAPMetricsAdapter {
    type ArrowMetricsStream =
        Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send + 'static>>;

    async fn arrow_metrics(
        &self,
        request: Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowMetricsStream>, Status> {
        let mut input_stream = request.into_inner();
        let receiver = self.receiver.clone();

        // Create a channel to send batch statuses back to the client
        let (tx, rx) = tokio::sync::mpsc::channel(100); // TODO?

        // Spawn a task to process incoming arrow records and convert them to OTLP
        #[allow(clippy::let_underscore_future)]
        let _ = tokio::spawn(async move {
            // Helper function to process a batch and send appropriate status
            async fn process_and_send(
                consumer: &mut Consumer,
                batch: &mut BatchArrowRecords,
                receiver: &TestReceiver<ExportMetricsServiceRequest>,
                tx: &tokio::sync::mpsc::Sender<Result<BatchStatus, Status>>,
            ) -> Result<(), ()> {
                let status_result = match process_arrow_metrics(consumer, batch, receiver).await {
                    Ok(_) => (StatusCode::Ok, "Successfully processed".to_string()),
                    Err(e) => (StatusCode::InvalidArgument, truncate_error(e.to_string())),
                };

                tx.send(Ok(BatchStatus {
                    batch_id: batch.batch_id,
                    status_code: status_result.0 as i32,
                    status_message: status_result.1,
                }))
                .await
                .map_err(|_| ())
            }

            let mut consumer = Consumer::default();
            // Process messages until stream ends or error occurs
            while let Ok(Some(mut batch)) = input_stream.message().await {
                // Process batch and send status, break on client disconnection
                if process_and_send(&mut consumer, &mut batch, &receiver, &tx)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Convert the mpsc receiver to a Stream
        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ArrowMetricsStream
        ))
    }
}


#[tonic::async_trait]
impl ArrowMetricsService for ArrowMetricsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let effect_handler_clone = self.effect_handler.clone();
        effect_handler_clone.send_message(OTLPRequest::Metrics(request.into_inner())).await;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl ArrowTraceService for ArrowTraceServiceImpl {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let effect_handler_clone = self.effect_handler.clone();
        effect_handler_clone.send_message(OTLPRequest::Traces(request.into_inner())).await;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

