//! CORS for browser clients served from another origin (e.g. `frontend/` on its own port).
//!
//! Allowed origins come from `/service/cors/allowedOrigins` (exact `scheme://host:port` strings,
//! `"*"` = any origin); no config = no CORS headers, i.e. browsers block cross-origin calls.
//! Preflights (`OPTIONS` + `Access-Control-Request-Method`) are answered here and never routed
//! to a service; a plain `OPTIONS` stays a public request.

use std::collections::HashSet;
use std::sync::LazyLock;

use ntex::{
    http::{
        Method, StatusCode,
        header::{self, HeaderValue},
    },
    web::{HttpRequest, HttpResponse},
};
use skw_lib_shared::{
    APP,
    prelude::consts::{HTTP_HEADER_API_KEY, HTTP_HEADER_PAYLOAD_SIGNATURE},
};

const CFG_PATH_CORS_ALLOWED_ORIGINS: &str = "/service/cors/allowedOrigins";
const ANY_ORIGIN: &str = "*";
const PREFLIGHT_MAX_AGE_SECS: &str = "600";
const ALLOW_METHODS: &str = "GET, HEAD, POST, PUT, PATCH, DELETE, QUERY";

static ALLOWED_ORIGINS: LazyLock<HashSet<String>> = LazyLock::new(|| {
    APP.config
        .get(CFG_PATH_CORS_ALLOWED_ORIGINS)
        .map(|v| {
            v.as_array()
                .unwrap_or_else(|| panic!("{CFG_PATH_CORS_ALLOWED_ORIGINS} must be an array"))
                .iter()
                .map(|o| {
                    o.as_str()
                        .unwrap_or_else(|| panic!("{CFG_PATH_CORS_ALLOWED_ORIGINS} must contain strings"))
                        .trim_end_matches('/')
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
});

static ALLOW_HEADERS: LazyLock<HeaderValue> = LazyLock::new(|| {
    HeaderValue::from_str(&format!(
        "Content-Type, {HTTP_HEADER_API_KEY}, {HTTP_HEADER_PAYLOAD_SIGNATURE}"
    ))
    .expect("valid Access-Control-Allow-Headers value")
});

/// Reads the config at startup, so a broken `/service/cors` fails fast instead of on the first request.
pub(crate) fn init() {
    LazyLock::force(&ALLOWED_ORIGINS);
}

/// The request's `Origin` if it is allowed, to be echoed in `Access-Control-Allow-Origin`.
pub(crate) fn allowed_origin(req: &HttpRequest) -> Option<HeaderValue> {
    let origin = req.headers().get(header::ORIGIN)?;
    let allowed = ALLOWED_ORIGINS.contains(ANY_ORIGIN) || origin.to_str().is_ok_and(|o| ALLOWED_ORIGINS.contains(o));

    allowed.then(|| origin.clone())
}

pub(crate) fn is_preflight(req: &HttpRequest) -> bool {
    req.method() == Method::OPTIONS
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
}

/// 204 for a preflight; without CORS headers when the origin isn't allowed, so the browser blocks the call.
pub(crate) fn preflight_response(origin: Option<HeaderValue>) -> HttpResponse {
    let mut resp = HttpResponse::new(StatusCode::NO_CONTENT);
    if let Some(origin) = origin {
        let headers = resp.headers_mut();
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static(ALLOW_METHODS),
        );
        headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, ALLOW_HEADERS.clone());
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static(PREFLIGHT_MAX_AGE_SECS),
        );
        add_headers(&mut resp, origin);
    }
    resp
}

/// Lets the page read this response.
pub(crate) fn add_headers(resp: &mut HttpResponse, origin: HeaderValue) {
    let headers = resp.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
}
