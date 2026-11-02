use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct Account {
    pub did: String,
    pub handle: String,
    pub display_name: Option<String>,
    pub avatar_cid: Option<String>,
    pub created_at: Option<String>,
    #[serde(default)]
    pub followers: u64,
}

pub fn read(path: &Path) -> anyhow::Result<Vec<Account>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    text.lines()
        .map(|line| serde_json::from_str(line).context("bad fixture line"))
        .collect()
}
