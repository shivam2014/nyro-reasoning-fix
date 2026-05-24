use super::*;
use crate::auth::AuthExchangeInput;
use crate::config::GatewayConfig;
use serde_json::json;
use std::path::PathBuf;
use uuid::Uuid;

const FAR_FUTURE_RFC3339: &str = "2099-01-01T00:00:00Z";
const PAST_RFC3339: &str = "2000-01-01T00:00:00Z";
const CODEX_RUNTIME_URL: &str = "https://chatgpt.com/backend-api/codex";

#[tokio::test]
async fn oauth_session_is_shared_across_admin_instances_and_cancel_deletes_it() -> anyhow::Result<()>
{
    let gw = build_gateway().await?;

    let init = gw.admin().init_oauth_session("codex", false).await?;
    let status = gw
        .admin()
        .get_oauth_session_status(&init.session_id)
        .await?;
    assert!(matches!(status, AuthSessionStatusData::Pending { .. }));

    gw.admin().cancel_oauth_session(&init.session_id).await?;
    assert!(
        gw.admin()
            .get_auth_session_record(&init.session_id)
            .await?
            .is_none()
    );

    let err = gw
        .admin()
        .get_oauth_session_status(&init.session_id)
        .await
        .expect_err("cancelled session should be removed");
    assert!(err.to_string().contains("auth session not found"));

    Ok(())
}

#[tokio::test]
async fn failed_complete_deletes_session() -> anyhow::Result<()> {
    let gw = build_gateway().await?;

    let init = gw.admin().init_oauth_session("codex", false).await?;
    let err = gw
        .admin()
        .complete_oauth_session(
            &init.session_id,
            AuthExchangeInput {
                code: None,
                callback_url: Some(
                    "https://app.example/callback?code=test-code&state=wrong-state".to_string(),
                ),
                metadata: Value::Null,
            },
        )
        .await
        .expect_err("invalid callback state should fail the exchange");

    assert!(
        err.to_string().contains("state"),
        "unexpected complete error: {err:#}"
    );
    assert!(
        gw.admin()
            .get_auth_session_record(&init.session_id)
            .await?
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn timeout_and_cleanup_remove_expired_sessions() -> anyhow::Result<()> {
    let gw = build_gateway().await?;

    let timed_out = gw.admin().init_oauth_session("codex", false).await?;
    gw.admin()
        .update_auth_session_record(
            &timed_out.session_id,
            UpdateAuthSession {
                expires_at: Some(PAST_RFC3339.to_string()),
                ..Default::default()
            },
        )
        .await?;

    let status = gw
        .admin()
        .get_oauth_session_status(&timed_out.session_id)
        .await?;
    assert!(matches!(
        status,
        AuthSessionStatusData::Error { ref code, .. } if code == "AUTH_TIMEOUT"
    ));
    assert!(
        gw.admin()
            .get_auth_session_record(&timed_out.session_id)
            .await?
            .is_none()
    );

    let stale_ready = gw.admin().init_oauth_session("codex", false).await?;
    seed_ready_session(
        &gw.admin(),
        &stale_ready.session_id,
        CredentialBundle {
            access_token: Some("stale-access-token".to_string()),
            refresh_token: Some("stale-refresh-token".to_string()),
            expires_at: Some(PAST_RFC3339.to_string()),
            resource_url: None,
            subject_id: None,
            scopes: vec![],
            raw: json!({ "access_token": "stale-access-token" }),
        },
    )
    .await?;

    let removed = gw.admin().cleanup_auth_sessions().await?;
    assert_eq!(removed, 1);
    assert!(
        gw.admin()
            .get_auth_session_record(&stale_ready.session_id)
            .await?
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn ready_session_is_single_use_and_provider_status_exposes_runtime_url() -> anyhow::Result<()>
{
    let gw = build_gateway().await?;

    let init = gw.admin().init_oauth_session("codex", false).await?;
    seed_ready_session(
        &gw.admin(),
        &init.session_id,
        CredentialBundle {
            access_token: Some("test-access-token".to_string()),
            refresh_token: Some("test-refresh-token".to_string()),
            expires_at: Some(FAR_FUTURE_RFC3339.to_string()),
            resource_url: None,
            subject_id: Some("acct_test".to_string()),
            scopes: vec!["openid".to_string(), "offline_access".to_string()],
            raw: json!({ "access_token": "test-access-token" }),
        },
    )
    .await?;

    let provider = gw
        .admin()
        .create_provider_with_oauth_session(&init.session_id, oauth_provider_input())
        .await?;

    assert_eq!(provider.effective_auth_mode(), "oauth");
    assert_eq!(provider.base_url, CODEX_RUNTIME_URL);
    assert!(
        gw.admin()
            .get_auth_session_record(&init.session_id)
            .await?
            .is_none()
    );

    let err = gw
        .admin()
        .create_provider_with_oauth_session(&init.session_id, oauth_provider_input())
        .await
        .expect_err("consumed ready session should not be reusable");
    assert!(err.to_string().contains("auth session not found"));

    let status = gw.admin().get_provider_oauth_status(&provider.id).await?;
    assert_eq!(status.status, AuthBindingStatus::Connected.as_str());
    assert_eq!(status.resource_url.as_deref(), Some(CODEX_RUNTIME_URL));

    Ok(())
}

async fn build_gateway() -> anyhow::Result<Gateway> {
    let config = GatewayConfig {
        data_dir: test_data_dir(),
        ..Default::default()
    };
    let (gw, _log_rx) = Gateway::new(config).await?;
    Ok(gw)
}

fn test_data_dir() -> PathBuf {
    std::env::temp_dir().join(format!("nyro-oauth-admin-tests-{}", Uuid::new_v4()))
}

fn oauth_provider_input() -> CreateProvider {
    CreateProvider {
        name: format!("oauth-provider-{}", Uuid::new_v4()),
        vendor: None,
        protocol: "openai".to_string(),
        base_url: "https://placeholder.invalid".to_string(),
        preset_key: Some("openai".to_string()),
        channel: Some("codex".to_string()),
        models_source: None,
        static_models: None,
        api_key: String::new(),
        auth_mode: "oauth".to_string(),
        use_proxy: false,
    }
}

async fn seed_ready_session(
    admin: &AdminService,
    session_id: &str,
    bundle: CredentialBundle,
) -> anyhow::Result<()> {
    admin
        .update_auth_session_record(
            session_id,
            UpdateAuthSession {
                status: Some(AuthSessionStatus::Ready.as_str().to_string()),
                result_json: Some(serde_json::to_string(&bundle)?),
                expires_at: bundle.expires_at.clone(),
                last_error: Some(String::new()),
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}
