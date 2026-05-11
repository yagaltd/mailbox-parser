use anyhow::{Context, Result};
use mailbox_parser::{
    ImapAccountConfig, ImapSyncOptions, ImapSyncState, SyncedEmail, sync_imap_delta,
    sync_imap_delta_streaming,
};

fn env_required(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn gmail_account_from_env() -> Option<ImapAccountConfig> {
    let host = env_required("GMAIL_IMAP_HOST")?;
    let port = env_required("GMAIL_IMAP_PORT")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(993);
    let username = env_required("GMAIL_IMAP_USER")?;
    let password = env_required("GMAIL_IMAP_APP_PASSWORD")?;
    let mailbox = env_required("GMAIL_IMAP_MAILBOX").unwrap_or_else(|| "INBOX".to_string());

    Some(ImapAccountConfig {
        host,
        username,
        password,
        port,
        tls: true,
        danger_skip_tls_verify: false,
        mailbox,
        account_id: Some("gmail-imap-smoke".to_string()),
        state_path: None,
    })
}

fn skip_msg() {
    eprintln!(
        "skip gmail_imap smoke: set GMAIL_IMAP_HOST/PORT/USER/APP_PASSWORD/MAILBOX env vars"
    );
}

fn msg_fingerprint(m: &SyncedEmail) -> (u32, Vec<String>, Option<u64>, Vec<String>, Option<u64>, Option<u32>) {
    (
        m.uid,
        m.flags.clone(),
        m.x_gm_thrid,
        m.x_gm_labels.clone(),
        m.modseq,
        m.rfc822_size,
    )
}

#[test]
#[ignore = "manual integration smoke; requires real Gmail IMAP credentials"]
fn gmail_imap_streaming_parity_matches_batch() -> Result<()> {
    let Some(account) = gmail_account_from_env() else {
        skip_msg();
        return Ok(());
    };

    let prior = ImapSyncState {
        uidvalidity: None,
        last_uid: 0,
        highest_modseq: None,
        last_sync_ms: 0,
    };
    let options = ImapSyncOptions {
        force_full: true,
        unseen_only: false,
    };

    let batch = sync_imap_delta(&account, &prior, options.clone()).context("batch sync")?;

    let mut streamed_messages = Vec::<SyncedEmail>::new();
    let mut on_message = |email: SyncedEmail| {
        streamed_messages.push(email);
        Ok(())
    };
    let streaming = sync_imap_delta_streaming(&account, &prior, options, &mut on_message)
        .context("streaming sync")?;

    assert_eq!(batch.account_id, streaming.account_id);
    assert_eq!(batch.mailbox, streaming.mailbox);
    assert_eq!(batch.state.uidvalidity, streaming.state.uidvalidity);
    assert_eq!(batch.state.last_uid, streaming.state.last_uid);
    assert_eq!(batch.state.highest_modseq, streaming.state.highest_modseq);
    assert_eq!(batch.supports_modseq, streaming.supports_modseq);
    assert_eq!(batch.server_capabilities, streaming.server_capabilities);
    assert_eq!(batch.vanished_uids, streaming.vanished_uids);
    assert_eq!(batch.mailbox_uids, streaming.mailbox_uids);

    assert!(streaming.messages.is_empty());
    assert_eq!(batch.messages.len(), streamed_messages.len());

    let mut batch_fp = batch.messages.iter().map(msg_fingerprint).collect::<Vec<_>>();
    let mut streaming_fp = streamed_messages
        .iter()
        .map(msg_fingerprint)
        .collect::<Vec<_>>();
    batch_fp.sort_by_key(|x| x.0);
    streaming_fp.sort_by_key(|x| x.0);

    assert_eq!(batch_fp, streaming_fp);

    Ok(())
}

#[test]
#[ignore = "manual integration smoke; requires real Gmail IMAP credentials"]
fn gmail_imap_reports_gmail_capability_and_metadata_shape() -> Result<()> {
    let Some(account) = gmail_account_from_env() else {
        skip_msg();
        return Ok(());
    };

    let prior = ImapSyncState {
        uidvalidity: None,
        last_uid: 0,
        highest_modseq: None,
        last_sync_ms: 0,
    };
    let options = ImapSyncOptions {
        force_full: false,
        unseen_only: false,
    };

    let res = sync_imap_delta(&account, &prior, options)?;
    assert!(
        res.server_capabilities
            .iter()
            .any(|c| c.contains("X-GM-EXT-1")),
        "expected X-GM-EXT-1 capability in server capabilities: {:?}",
        res.server_capabilities
    );

    for m in &res.messages {
        for label in &m.x_gm_labels {
            assert!(
                !label.trim().is_empty(),
                "gmail label should not be blank for uid {}",
                m.uid
            );
        }
    }

    Ok(())
}
