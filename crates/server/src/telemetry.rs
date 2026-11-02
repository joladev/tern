use anyhow::Context as _;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_process::Collector;

const SEARCH_BUCKETS: &[f64] = &[
    0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
    0.1,
];
const LOOKUP_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const COMMIT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub struct Exporter {
    handle: PrometheusHandle,
    process: Collector,
}

impl Exporter {
    pub fn install() -> anyhow::Result<Exporter> {
        let handle = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("tern_search_seconds".to_owned()),
                SEARCH_BUCKETS,
            )
            .and_then(|builder| {
                builder.set_buckets_for_metric(
                    Matcher::Full("tern_handle_lookup_seconds".to_owned()),
                    LOOKUP_BUCKETS,
                )
            })
            .and_then(|builder| {
                builder.set_buckets_for_metric(
                    Matcher::Full("tern_commit_seconds".to_owned()),
                    COMMIT_BUCKETS,
                )
            })
            .context("failed to configure the metric buckets")?
            .install_recorder()
            .context("failed to install the metrics recorder")?;
        let process = Collector::default();
        process.describe();
        Ok(Exporter { handle, process })
    }

    pub fn render(&self) -> String {
        self.process.collect();
        self.handle.render()
    }
}
