use bytes::Bytes;
#[allow(unused_imports)]
use skw_lib_shared::{
    APP, AppError,
    prelude::{
        base64::*,
        chrono::*,
        consts::*,
        futures::StreamExt,
        iggy::*,
        jsonrpc::{JsonRpcGateHttpRequest, SkwServiceHttpError},
        log::{debug, error, info, trace, warn},
        serde::*,
        strum::EnumString,
        uuid::Uuid,
    },
    skw_get_consumer_name, skw_random_signature,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, LazyLock, atomic::AtomicU64},
};

use tokio::sync::broadcast::{Receiver, Sender};

use ntex::{
    http::Method,
    util::Bytes as NtexBytes,
    web::{self, HttpRequest, HttpResponse, HttpResponseBuilder},
};

use crate::cors;

static PUBLIC_REQ_VALUE: LazyLock<Value> = LazyLock::new(|| json!({"Method": "Public", "Params": {}}));

#[derive(Clone)]
pub struct RoutesMap {
    pub map: HashMap<String, Arc<IggyProducer>>,
}

struct IggySendResult {
    pub rx: Receiver<IggyReceivedMessage>,
    pub _ts_client: Option<DateTime<Utc>>,
    pub req_signature: String,
    pub _ts_gate_received: Option<DateTime<Utc>>,
    pub _ts_gate_published: DateTime<Utc>,
}

#[derive(Clone)]
pub struct GateState {
    pub auth_service_url: String,
    pub routes_map: RoutesMap,
    pub back_sender: Sender<IggyReceivedMessage>,
    pub _iggy_client: Arc<IggyClient>, // saved for future use if needed
    pub _cur_id: Arc<AtomicU64>,
}
#[derive(Debug, Clone)]
pub struct IggyReceivedMessage {
    pub payload: Vec<u8>,
    pub signature: Option<String>,
    pub service_error: Option<SkwServiceHttpError>,
    pub _ts_service_received: Option<DateTime<Utc>>,
    pub _ts_service_sent: Option<DateTime<Utc>>,
    pub _ts_gate_back_received: DateTime<Utc>,
}

impl TryFrom<ReceivedMessage> for IggyReceivedMessage {
    type Error = AppError;

    fn try_from(m: ReceivedMessage) -> Result<Self, Self::Error> {
        let ts_gate_back_received = Utc::now();

        let signature_key = HeaderKey::from_str(IGGY_HEADER_SIGNATURE)?;

        let signature = m
            .message
            .get_user_header(&signature_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value());

        let service_error_key = HeaderKey::from_str(IGGY_HEADER_SERVICE_ERROR)?;

        let service_error = m
            .message
            .get_user_header(&service_error_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| v.parse::<SkwServiceHttpError>().ok());

        let ts_service_received_key = HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_RECEIVED)?;
        let ts_service_received = m
            .message
            .get_user_header(&ts_service_received_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
            .map(|v| v.with_timezone(&Utc));

        let ts_service_sent_key = HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_SENT)?;
        let ts_service_sent = m
            .message
            .get_user_header(&ts_service_sent_key)
            .unwrap_or(None)
            .map(|s| s.to_string_value())
            .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
            .map(|v| v.with_timezone(&Utc));

        Ok(IggyReceivedMessage {
            payload: m.message.payload.to_vec(),
            signature,
            service_error,
            _ts_service_received: ts_service_received,
            _ts_service_sent: ts_service_sent,
            _ts_gate_back_received: ts_gate_back_received,
        })
    }
}

impl GateState {
    pub async fn new(
        auth_service_url: &str,
        stream: &str,
        back_topic: &str,
        routes_map: RoutesMap,
        arc_iggy_client: Arc<IggyClient>,
        back_topic_batch_length: u32,
        back_topic_consumers: u32,
    ) -> Result<Self, AppError> {
        // Back-topic consumer group "consumer-{back_topic}". gates/ws reads the same
        // back topic under its own group ("consumer-ws-{back_topic}"); different
        // groups each get the full message stream, so neither gate can steal the
        // other's replies — each just drops messages nobody here is waiting for.
        // Keep the two names distinct.
        //
        // Caveat: every api-gate *process* uses this same group name, so running
        // several api-gate replicas puts them in one group and Iggy splits back
        // partitions between them — a reply for a request waiting on replica A can
        // land on replica B, get dropped there, and A times out. Before scaling
        // horizontally, make the group per-instance or route replies to a
        // partition owned by the requesting instance. (The N workers below are
        // fine: they're one process and all feed the same broadcast channel.)
        let consumer_name = skw_get_consumer_name(back_topic);

        let (ts, _) = tokio::sync::broadcast::channel::<IggyReceivedMessage>(1000);

        // Each back-topic consumer below is its own IggyClient connection (not a
        // clone of arc_iggy_client): the Iggy server tracks consumer-group
        // membership per connection, so N consumers sharing one connection would
        // all register as the same group member and the server would never split
        // partitions across them. Separate connections is what actually gets
        // partitions assigned in parallel across workers.
        for worker_id in 0..back_topic_consumers.max(1) {
            let stream = stream.to_string();
            let back_topic = back_topic.to_string();
            let consumer_name = consumer_name.clone();
            let consumer_ts = ts.clone();

            tokio::spawn(async move {
                loop {
                    let iggy = APP.iggy.clone().connect().await;
                    let Some(client) = iggy.inner() else {
                        error!("back receiver #{worker_id}: no iggy client available");
                        return;
                    };

                    let mut iggy_consumer = match client.consumer_group(&consumer_name, &stream, &back_topic) {
                        Ok(builder) => {
                            let poll_interval = match IggyDuration::from_str("1ms") {
                                Ok(i) => i,
                                Err(e) => {
                                    error!("back receiver #{worker_id}: invalid poll interval: {e}");
                                    return;
                                }
                            };
                            builder
                                .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
                                .create_consumer_group_if_not_exists()
                                .auto_join_consumer_group()
                                .polling_strategy(PollingStrategy::next())
                                .poll_interval(poll_interval)
                                .batch_length(back_topic_batch_length)
                                .build()
                        }
                        Err(e) => {
                            error!("back receiver #{worker_id}: failed to build consumer: {e}");
                            return;
                        }
                    };

                    if let Err(e) = iggy_consumer.init().await {
                        error!("back receiver #{worker_id}: failed to init consumer: {e}");
                        return;
                    }

                    info!(
                        "starting back receiver thread #{worker_id} {}",
                        consumer_name
                    );
                    while let Some(message) = iggy_consumer.next().await {
                        match message {
                            Ok(received) => match received.try_into() {
                                Ok(m) => {
                                    // Err only means "no receivers right now" (no request in flight, a reply meant
                                    // for the ws gate, or a late reply after timeout). Not a failure.
                                    if consumer_ts.send(m).is_err() {
                                        debug!("back message dropped: no request waiting for it");
                                    }
                                }
                                Err(e) => error!("error converting back message: {}", e),
                            },
                            Err(e) => error!("Error while receiving message: {}", e),
                        }
                    }
                }
            });
        }

        Ok(Self {
            auth_service_url: auth_service_url.into(),
            routes_map,
            back_sender: ts,
            _iggy_client: arc_iggy_client,
            _cur_id: Arc::new(AtomicU64::new(0)),
        })
    }
}

pub(crate) async fn catch_all(state: web::types::State<GateState>, req: HttpRequest, body: NtexBytes) -> HttpResponse {
    let cors_origin = cors::allowed_origin(&req);
    if cors::is_preflight(&req) {
        return cors::preflight_response(cors_origin);
    }

    let mut resp = handle_request(state, req, body).await;
    if let Some(origin) = cors_origin {
        cors::add_headers(&mut resp, origin);
    }
    resp
}

async fn handle_request(state: web::types::State<GateState>, req: HttpRequest, body: NtexBytes) -> HttpResponse {
    let ts_gate_received = Utc::now();

    let method = req.method();
    let mut slug_split = req.path().trim_matches('/').split("/");
    let slug1 = slug_split.next();
    let slug2 = slug_split.next();

    let topic_key = match (slug1, slug2) {
        (None, _) => return HttpResponse::BadRequest().into(),
        (Some(s1), None) => s1,
        (Some(s1), Some(s2)) => &format!("{s1}/{s2}"),
    };

    let producer = match state.routes_map.map.get(topic_key) {
        None => return HttpResponse::BadRequest().into(),
        Some(p) => p.clone(),
    };

    let send_result = match *method {
        Method::CONNECT | Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE => match slug2.unwrap_or_default().try_into() {
            Ok(SkwTopicType::Public) => send_public_request(&state, producer, method, req.path()).await,
            _ => return HttpResponse::BadRequest().into(),
        },
        Method::PATCH | Method::POST | Method::PUT | Method::DELETE | Method::QUERY => match slug2.unwrap_or_default().try_into() {
            Ok(SkwTopicType::Open) => send_open_request(&state, producer, &req, method, &body).await,
            Ok(SkwTopicType::Signed) => send_signed_request(&state, producer, &req, method, &body).await,
            _ => return HttpResponse::BadRequest().into(),
        },
        _ => return HttpResponse::BadRequest().into(),
    };

    let mut sr = match send_result {
        Ok(r) => r,
        Err(e) => return e,
    };

    sr._ts_gate_received = Some(ts_gate_received);
    await_service_response(sr, method, req.path()).await
}

async fn send_public_request(state: &GateState, producer: Arc<IggyProducer>, method: &Method, path: &str) -> Result<IggySendResult, HttpResponse> {
    // no headers check is needed

    // generate random signature
    let signature = skw_random_signature();

    // generate signature generation on sequence
    // let next_id = state
    //     .cur_id
    //     .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    //     + 1;
    // let signature = BASE64_STANDARD.encode(next_id.to_be_bytes());

    // use constant RpcMethod for all types of requests
    let Ok(public_req_body) = serde_json::to_vec(&JsonRpcGateHttpRequest {
        ts: Utc::now(),
        method: PUBLIC_REQ_VALUE.clone(),
    }) else {
        return Err(HttpResponse::Gone().into());
    };

    let rx = state.back_sender.subscribe(); // prevent late subscription error!
    match send_message_to_service(producer, None, &signature, method, path, &public_req_body).await {
        Ok(_ts_gate_published) => Ok(IggySendResult {
            rx,
            _ts_client: None,
            req_signature: signature,
            _ts_gate_published,
            _ts_gate_received: None,
        }),
        Err(e) => {
            error!("{:?}", e);
            Err(e)
        }
    }
}

async fn send_open_request(
    state: &GateState,
    producer: Arc<IggyProducer>,
    req: &HttpRequest,
    method: &Method,
    body: &[u8],
) -> Result<IggySendResult, HttpResponse> {
    let Ok(str_body) = std::str::from_utf8(body) else {
        return Err(HttpResponse::BadRequest().into());
    };

    let _ts_client = match serde_json::from_str::<JsonRpcGateHttpRequest>(str_body) {
        Ok(r) => Some(r.ts),
        // valid JSON that doesn't match the struct (e.g. missing `Ts`) - allowed for open requests
        Err(e) if e.is_data() => None,
        // syntax error / truncated input - not JSON at all
        Err(_) => return Err(HttpResponse::BadRequest().into()),
    };

    // let api_key = req
    //     .headers()
    //     .get(HTTP_HEADER_API_KEY)
    //     .and_then(|k| Uuid::try_parse_ascii(k.as_bytes()).ok())
    //     .filter(|u| u.get_version_num() == 7);

    let req_signature = skw_random_signature();

    let rx = state.back_sender.subscribe();
    let _ts_gate_published = send_message_to_service(
        producer,
        // api_key.as_ref(),
        None,
        &req_signature,
        method,
        req.path(),
        body,
    )
    .await?;

    Ok(IggySendResult {
        rx,
        _ts_client,
        req_signature,
        _ts_gate_published,
        _ts_gate_received: None,
    })
}

async fn send_signed_request(
    state: &GateState,
    producer: Arc<IggyProducer>,
    req: &HttpRequest,
    method: &Method,
    body: &[u8],
) -> Result<IggySendResult, HttpResponse> {
    let Ok(str_body) = std::str::from_utf8(body) else {
        return Err(HttpResponse::BadRequest().into());
    };

    let ts_client = match serde_json::from_str::<JsonRpcGateHttpRequest>(str_body) {
        Ok(r) => r.ts,
        Err(_) => return Err(HttpResponse::BadRequest().into()),
    };

    let (api_key, req_signature) = match crate::auth::check_auth_key_and_request_signature(&state.auth_service_url, req, str_body).await {
        Ok(s) => s,
        Err(e) => return Err(app_error_to_http_response(req, e)),
    };

    let rx = state.back_sender.subscribe();
    let _ts_gate_published = send_message_to_service(
        producer,
        Some(&api_key),
        &req_signature,
        method,
        req.path(),
        body,
    )
    .await?;

    Ok(IggySendResult {
        rx,
        _ts_client: Some(ts_client),
        req_signature,
        _ts_gate_published,
        _ts_gate_received: None,
    })
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
async fn await_service_response(mut sr: IggySendResult, _method: &Method, _path: &str) -> HttpResponse {
    let signature = sr.req_signature.clone();
    let wait_for_match = async move {
        loop {
            match sr.rx.recv().await {
                Ok(m) if m.signature.is_some() => {
                    if *m.signature.as_ref().unwrap_or(&"".to_string()) == *sr.req_signature {
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
            /*
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
            */

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
#[allow(unused)]
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
    trace!(
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

fn service_error_to_http_response(srv_err: SkwServiceHttpError) -> HttpResponseBuilder {
    use SkwServiceHttpError::*;
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
