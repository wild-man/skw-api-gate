use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, LazyLock},
};

//@todo benchmark and compare with standard allocator
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub const CFG_PATH_BACK_TOPIC_PART_COUNT: &str = "/service/backTopicpartitionsCount";

use ntex::{
    SharedCfg,
    http::{HttpServiceConfig, Method},
    time::Seconds,
    util::Bytes as NtexBytes,
    web::{self, App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer},
};

use skw_lib_shared::{
    APP, AppError,
    prelude::{
        base64::*,
        chrono::*,
        consts::*,
        iggy::*,
        jsonrpc::{JsonRpcGateHttpRequest, ServiceHttpError},
        log::{error, info, warn},
        serde::*,
        tools::*,
        uuid::Uuid,
    },
};

use tokio::sync::broadcast::Receiver;

use crate::gate::{GateState, IggyReceivedMessage, RoutesMap};

mod auth;
mod gate;
mod tls;

static PUBLIC_REQ_VALUE: LazyLock<Value> = LazyLock::new(|| json!({"Method": "Public", "Params": {}}));

#[ntex::main]
async fn main() -> anyhow::Result<()> {
    let bind_addr = APP.config.expect_string(SERVICE_BIND_ADDRESS);
    let back_topic = skw_get_back_topic_name();
    let stream = skw_get_stream_name();

    let workers = APP
        .config
        .get(SERVICE_WORKERS)
        .map(|w| w.as_u64().expect("workers is not a number") as usize)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(2, std::num::NonZeroUsize::get));

    let auth_service_url = APP
        .config
        .get(AUTH_SREVICE_URL)
        .map(|a| {
            a.as_str()
                .expect("invalid /service/authServiceUrl")
                .to_string()
        })
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| skw_lib_http_protos::auth::DEFAULT_SERVICE_URL.to_string());
    let auth_service_url = format!("{}{}", auth_service_url, INTERNAL_URL_PREFIX);

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("unable to install rustls aws-lc-rs CryptoProvider");

    let tls_config = tls::build_rustls_config()?;

    //@todo move values into config
    let http_cfg = HttpServiceConfig::new()
        .set_client_timeout(Seconds::new(3))
        .set_keepalive_timeout(Seconds::new(20))
        .set_headers_read_rate(Seconds::new(1), Seconds::new(3), 1)
        .set_payload_read_rate(Seconds::new(5), Seconds::new(10), 1);

    let shared_http_cfg = SharedCfg::new("api-gate").add(http_cfg);

    let iggy = APP.iggy.clone().connect().await;
    let iggy_client = iggy.inner().expect("bad iggy client");

    let routes_map = init_iggy(&stream, &back_topic, &iggy_client).await?;

    let gate_state = GateState::new(
        &auth_service_url,
        &stream,
        &back_topic,
        routes_map,
        iggy_client,
    )
    .await?;

    info!("starting skw-api-gate listening on https://{}", bind_addr);
    HttpServer::new(async move || {
        App::new()
            .state(gate_state.clone())
            .default_service(web::route().to(catch_all))
    })
    .bind_rustls(&bind_addr, &tls_config)?
    .config(shared_http_cfg)
    .workers(workers)
    .run()
    .await?;

    Ok(())
}

async fn init_iggy(stream: &str, back_topic: &str, iggy_client: &IggyClient) -> Result<RoutesMap, AppError> {
    let stream_id = Identifier::from_str_value(stream).expect("invalid stream name");

    match iggy_client.create_stream(stream).await {
        Ok(sd) => info!("iggy stream {} created; id:{}", stream, sd.id),
        Err(e) => warn!("failed to create iggy stream {}: {}", stream, e),
    };
    let partitions_count = APP.config.expect_u32(CFG_PATH_BACK_TOPIC_PART_COUNT);

    match iggy_client
        .create_topic(
            &stream_id,
            back_topic,
            partitions_count,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::ExpireDuration(IggyDuration::from_str("60s").unwrap()),
            MaxTopicSize::ServerDefault,
        )
        .await
    {
        Ok(tc) => info!("gate-back topic created: {:?}", tc),
        Err(e) => warn!("Error gate-back topic creation: {}", e),
    }

    let mut routes_map = RoutesMap {
        map: HashMap::with_capacity(100),
    };

    match APP.config.get(GATE_TOPICS_MAP) {
        Some(t) => {
            for (service, topic_cfg) in t.as_object().expect("invalid topic configuration").iter() {
                // dbg!((service_name, topic_cfg));
                let cfg: TopicConfig = topic_cfg.try_into()?;

                let topic = cfg
                    .custom_name
                    .unwrap_or(format!("{}-{}", service, VERSION));

                skw_is_valid_name(&topic).unwrap_or_else(|| panic!("invalid topic name: {}", topic));

                match iggy_client
                    .create_topic(
                        &stream_id,
                        &topic,
                        cfg.partitions_count,
                        cfg.compression_algorithm,
                        None,
                        cfg.message_expiry,
                        cfg.max_topic_size,
                    )
                    .await
                {
                    Ok(t) => info!("created topic: {} (id:{}); stream: {}", topic, t.id, stream),
                    Err(e) => warn!(
                        "error create topic: {}; stream: {}; error: {}",
                        topic, stream, e
                    ),
                }

                info!(
                    "creating producer for service: {}; stream: {}; topic: {};",
                    service, stream, topic
                );

                let producer = iggy_client
                    .producer(stream, &topic)
                    .expect("producer create error") // stream: "orders", topic: "events"
                    .partitioning(Partitioning::balanced()) // Automatically balances across partitions
                    .build();

                routes_map
                    .map
                    .entry(service.clone())
                    .or_insert(Arc::new(producer));
            }
        }
        None => warn!("no topics configurations found in config"),
    }

    Ok(routes_map)
}

async fn catch_all(state: web::types::State<GateState>, req: HttpRequest, body: NtexBytes) -> HttpResponse {
    let ts_gate_received = Utc::now();

    let method = req.method();
    let slug1 = req.path().split("/").nth(1);

    let producer = match slug1 {
        None => return HttpResponse::BadRequest().into(),
        Some(s) => {
            let p = state.routes_map.map.get(s);
            match p {
                None => return HttpResponse::BadRequest().into(),
                Some(pp) => pp.clone(),
            }
        }
    };

    let (ts_client, req_signature, ts_gate_published) = match *method {
        Method::CONNECT | Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE => {
            // no headers check is needed

            // info!("{} {}", method, req.path());

            // generate random signature
            // let mut rand_signature_bytes = [0u8; 64];
            // rand::thread_rng().fill_bytes(&mut rand_signature_bytes);
            // let rand_signature = BASE64_STANDARD.encode(rand_signature_bytes);

            let next_id = state
                .cur_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            let cnt_signature = BASE64_STANDARD.encode(next_id.to_be_bytes());

            // use constant RpcMethod for all types of requests
            let Ok(public_req_body) = serde_json::to_vec(&JsonRpcGateHttpRequest {
                ts: Utc::now(),
                method: PUBLIC_REQ_VALUE.clone(),
            }) else {
                return HttpResponse::Gone().into();
            };

            match send_message_to_service(
                producer,
                None,
                &cnt_signature,
                method,
                req.path(),
                &public_req_body,
            )
            .await
            {
                Ok(ts_gate_published) => (None, cnt_signature.to_string(), ts_gate_published),
                Err(e) => {
                    error!("{:?}", e);
                    return e;
                }
            }
        }
        Method::PATCH | Method::POST | Method::PUT | Method::DELETE | Method::QUERY => {
            let Ok(str_body) = std::str::from_utf8(&body) else {
                return HttpResponse::BadRequest().into();
            };

            let ts_client = match serde_json::from_str::<JsonRpcGateHttpRequest>(str_body) {
                Err(_) => return HttpResponse::BadRequest().into(),
                Ok(r) => r.ts,
            };

            let (api_key, req_signature) = match auth::check_auth_key_and_request_signature(&state.auth_service_url, &req, str_body).await {
                Err(e) => return app_error_to_http_response(&req, e),
                Ok(s) => s,
            };

            match send_message_to_service(
                producer,
                Some(&api_key),
                &req_signature,
                method,
                req.path(),
                &body,
            )
            .await
            {
                Ok(ts_gate_published) => (Some(ts_client), req_signature, ts_gate_published),
                Err(e) => return e,
            }
        }
        _ => return HttpResponse::BadRequest().into(),
    };

    let rx = state.back_sender.subscribe();
    await_service_response(
        rx,
        &req_signature,
        method,
        req.path(),
        ts_client,
        ts_gate_received,
        ts_gate_published,
    )
    .await
}

async fn send_message_to_service(
    producer: Arc<IggyProducer>,
    api_key: Option<&Uuid>,
    signature: &str,
    method: &Method,
    path: &str,
    body: &[u8],
) -> Result<DateTime<Utc>, HttpResponse> {
    let mut headers = BTreeMap::new();

    headers.insert(
        HeaderKey::from_str(IGGY_HEADER_SIGNATURE).unwrap(),
        HeaderValue::from_str(signature).map_err(|e| {
            error!("iggy header error IGGY_HEADER_SIGNATURE {}", e);
            HttpResponse::InternalServerError()
        })?,
    );

    if let Some(key) = api_key {
        headers.insert(
            HeaderKey::from_str(IGGY_HEADER_API_KEY).unwrap(),
            HeaderValue::from_str(&key.to_string()).map_err(|e| {
                error!("iggy header error IGGY_HEADER_API_KEY {}", e);
                HttpResponse::InternalServerError()
            })?,
        );
    }

    headers.insert(
        HeaderKey::from_str(IGGY_HEADER_HTTP_METHOD).unwrap(),
        HeaderValue::from_str(method.as_str()).map_err(|e| {
            error!("iggy header error IGGY_HEADER_HTTP_METHOD {}", e);
            HttpResponse::InternalServerError()
        })?,
    );

    headers.insert(
        HeaderKey::from_str(IGGY_HEADER_HTTP_PATH).unwrap(),
        HeaderValue::from_str(path).map_err(|e| {
            error!("iggy header error IGGY_HEADER_HTTP_PATH {}", e);
            HttpResponse::InternalServerError()
        })?,
    );

    let bytes = Bytes::copy_from_slice(body); //@todo try to remove copy_from_slice
    let message = IggyMessage::builder()
        .payload(bytes)
        .user_headers(headers)
        .build();

    if let Err(e) = message {
        error!("Iggy build message error: {}", e);
        return Err(HttpResponse::InternalServerError().into());
    }

    if let Err(e) = producer.send_one(message.unwrap()).await {
        error!("sending to iggy error: {}", e);
        return Err(HttpResponse::InternalServerError().into());
    }

    Ok(Utc::now())
}

#[allow(clippy::too_many_arguments)]
async fn await_service_response(
    mut rx: Receiver<IggyReceivedMessage>,
    signature: &str,
    method: &Method,
    path: &str,
    ts_client: Option<DateTime<Utc>>,
    ts_gate_received: DateTime<Utc>,
    ts_gate_published: DateTime<Utc>,
) -> HttpResponse {
    let wait_for_match = async move {
        loop {
            match rx.recv().await {
                Ok(m) if m.signature.is_some() => {
                    if m.signature.as_ref().unwrap() == signature {
                        return Ok(m);
                    }
                    continue;
                }
                Ok(_) => continue,
                Err(e) => return Err(e),
            }
        }
    };

    match tokio::time::timeout(BACK_MESSAGE_TIMEOUT, wait_for_match).await {
        Ok(Ok(m)) => {
            log_request_timing(
                signature,
                method,
                path,
                ts_client,
                ts_gate_received,
                ts_gate_published,
                m.ts_service_received,
                m.ts_service_sent,
                m.ts_gate_back_received,
                Utc::now(),
            );

            match m.service_error {
                None => HttpResponse::Ok().body(m.payload),
                // add body from service response to transfer original service response to user
                Some(s) => service_error_to_http_response(s).body(m.payload),
            }
        }
        Ok(Err(e)) => {
            error!("back channel error: {}", e);
            HttpResponse::InternalServerError().into()
        }
        Err(_) => {
            warn!(
                "timeout waiting for backend response signature: {}",
                signature
            );
            HttpResponse::GatewayTimeout().into()
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn log_request_timing(
    signature: &str,
    method: &Method,
    path: &str,
    ts_client: Option<DateTime<Utc>>,
    ts_gate_received: DateTime<Utc>,
    ts_gate_published: DateTime<Utc>,
    ts_service_received: Option<DateTime<Utc>>,
    ts_service_sent: Option<DateTime<Utc>>,
    ts_gate_back_received: DateTime<Utc>,
    ts_gate_sent: DateTime<Utc>,
) {
    info!(
        target: "request_timing",
        "{}",
        json!({
            "signature": signature,
            "method": method.as_str(),
            "path": path,
            "ts_client": ts_client,
            "ts_gate_received": ts_gate_received,
            "ts_gate_published": ts_gate_published,
            "ts_service_received": ts_service_received,
            "ts_service_sent": ts_service_sent,
            "ts_gate_back_received": ts_gate_back_received,
            "ts_gate_sent": ts_gate_sent,
        })
    );
}

fn service_error_to_http_response(srv_err: ServiceHttpError) -> HttpResponseBuilder {
    use ServiceHttpError::*;
    match srv_err {
        NotFound => HttpResponse::NotFound(),
        Forbidden => HttpResponse::Forbidden(),
        BadRequest => HttpResponse::BadRequest(),
        UnprocessableEntity => HttpResponse::UnprocessableEntity(),
        InternalServerError => HttpResponse::InternalServerError(),
    }
}

fn internal_error(_req: &HttpRequest, e: String) -> HttpResponse {
    error!("internal error: {}", e);
    HttpResponse::InternalServerError().into()
}

fn app_error_to_http_response(req: &HttpRequest, app_error: AppError) -> HttpResponse {
    use AppError::*;
    match app_error {
        Service(s) => service_error_to_http_response(s).into(),
        Custom(e) => internal_error(req, e),
        e => internal_error(req, e.to_string()),
    }
}
