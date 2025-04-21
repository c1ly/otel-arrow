use grpc_stubs::proto::experimental::arrow::v1::{
    arrow_logs_service_server::{ArrowLogsService},
    arrow_traces_service_server::{ArrowTracesService},
    arrow_metrics_service_server::{ArrowMetricsService}

};
use tonic::{Request, Response, Status};
use otap_df_engine::receiver::{EffectHandler, SendableMode};


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
                #[path = "opentelemetry.proto.collector.logs.v1.rs"]
                pub mod v1;
            }
        }
    }
}


pub struct ArrowLogsServiceImpl {
    effect_handler: EffectHandler<OTAPRequest, SendableMode>,
}

impl ArrowLogsServiceImpl {
    pub fn new(effect_handler: EffectHandler<OTAPRequest, SendableMode>) -> Self {
        Self { effect_handler }
    }
}
pub struct ArrowMetricsServiceImpl {
    effect_handler: EffectHandler<OTAPRequest, SendableMode>,
}

impl ArrowMetricsServiceImpl {
    pub fn new(effect_handler: EffectHandler<OTAPRequest, SendableMode>) -> Self {
        Self { effect_handler }
    }
}
pub struct ArrowTraceServiceImpl {
    effect_handler: EffectHandler<OTAPRequest, SendableMode>,
}

impl ArrowTraceServiceImpl {
    pub fn new(effect_handler: EffectHandler<OTAPRequest, SendableMode>) -> Self {
        Self { effect_handler }
    }
}

#[tonic::async_trait]
impl ArrowLogsService for ArrowLogsServiceImpl {
    type ArrowLogsStream = 
    async fn arrow_logs(
        &self,
        request: Request<tonic::Streaming<super::BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowLogsStream>, tonic::Status> {


        let effect_handler_clone = self.effect_handler.clone();
        effect_handler_clone.send_message(OTAPRequest::Logs(request.into_inner())).await;
        tokio::spawn_local(async move {
            while let Some(ping) = req_stream.next().await {
                let ping = ping.unwrap();
                println!("Message recieved: {}", ping.message);
                let num_str = ping.message.split(':').next_back().unwrap();
                let num: u32 = num_str.trim().parse().unwrap();
                *index.write().unwrap() = num + 1;
                let pong = *index.read().unwrap();
                tx.send(Ok(Pong { pong })).await.unwrap();
            }
        });
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
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

