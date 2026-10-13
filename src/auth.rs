use cached::{TtlCache, cached};
use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use ntex::web::HttpRequest;
use skw_lib_shared::{
    AppError,
    prelude::{
        base64::*,
        consts::{HTTP_HEADER_API_KEY, HTTP_HEADER_PAYLOAD_SIGNATURE},
        jsonrpc::{ServiceHttpError, internal_http_request},
        uuid::Uuid,
    },
};
use std::time::Duration;

use skw_lib_http_protos::auth::internal::*;

pub(crate) async fn check_auth_key_and_request_signature(url: &str, req: &HttpRequest, body: &str) -> Result<(Uuid, String), AppError> {
    let key = req
        .headers()
        .get(HTTP_HEADER_API_KEY)
        .and_then(|k| Uuid::try_parse_ascii(k.as_bytes()).ok())
        .filter(|u| u.get_version_num() == 7)
        .ok_or(ServiceHttpError::Forbidden)?;

    let h_signature = req.headers().get(HTTP_HEADER_PAYLOAD_SIGNATURE);

    let signature = h_signature
        .and_then(|v| BASE64_STANDARD.decode(v.as_bytes()).ok())
        .and_then(|v| Signature::from_slice(&v).ok())
        .ok_or(ServiceHttpError::Forbidden)?;

    let auth_req = HttpMethod::GetKeyById { id: key };
    let HttpPayload::GetKeyById {
        public_key,
        enabled,
        ..
    } = internal_http_request_cached(url, &key, auth_req)
        .await
        // unknown key is reported like any other auth failure, so clients can't probe which key ids exist;
        // other errors (auth service down, ...) stay internal errors
        .map_err(|e| match e {
            AppError::Service(ServiceHttpError::NotFound) => ServiceHttpError::Forbidden.into(),
            e => e,
        })?
        .try_into()?;

    if !enabled {
        return Err(ServiceHttpError::Forbidden.into());
    }

    let public_key_der = BASE64_STANDARD
        .decode(public_key)
        .map_err(|_| ServiceHttpError::Forbidden)?;

    let verifying_key = VerifyingKey::from_public_key_der(&public_key_der).map_err(|_| ServiceHttpError::Forbidden)?;

    verifying_key
        .verify(body.as_bytes(), &signature)
        .map_err(|_| ServiceHttpError::Forbidden)?;

    // h_signature is base64 encoded string contains valid signature, so unwrap is safe
    let ret_signature = h_signature.unwrap().to_str().unwrap().into();
    Ok((key, ret_signature))
}

#[cached(
    convert = r#"{ format!("{}", _key) }"#,
    ty = "TtlCache<String, String>",
    create = "{ TtlCache::new(Duration::from_secs(60)) }"
)]
async fn internal_http_request_cached(url: &str, _key: &Uuid, req: HttpMethod) -> Result<String, AppError> {
    internal_http_request(url, req).await
}
