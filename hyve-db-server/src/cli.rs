use std::{
    fmt::{self, Display},
    time::Duration,
};

use clap::Parser;
use clap_num::number_range;
use rand::Rng as _;

#[derive(Debug, Clone)]
pub struct DurationRange(Duration, Duration);

impl DurationRange {
    const fn new(min: Duration, max: Duration) -> Self {
        DurationRange(min, max)
    }

    pub fn select_random(&self) -> Duration {
        rand::rng().random_range(self.0..self.1)
    }

    fn parse(input: &str) -> anyhow::Result<Self> {
        let [min, max] = input
            .split(",")
            .map(humantime::parse_duration)
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| {
                anyhow::anyhow!("Duration range must contain two durations separated by a comma")
            })?;
        Ok(DurationRange(min, max))
    }
}

impl Display for DurationRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{}",
            humantime::format_duration(self.0),
            humantime::format_duration(self.1)
        )
    }
}

fn max_nodes_parser(s: &str) -> Result<usize, String> {
    number_range(s, 3, 500)
}

fn replication_factor_parser(s: &str) -> Result<usize, String> {
    number_range(s, 3, 10)
}

#[derive(Debug, Clone, Parser)]
pub struct Cli {
    #[arg(long, value_parser = max_nodes_parser)]
    pub max_nodes: usize,
    #[arg(long)]
    pub port: u16,
    #[arg(long, value_delimiter = ',', num_args = 1)]
    pub peers: Vec<u16>,
    #[arg(long, value_parser = replication_factor_parser)]
    pub replication_factor: usize,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "100ms")]
    pub heartbeat_interval: Duration,
    #[arg(
        long,
        value_parser = DurationRange::parse,
        default_value_t = DurationRange::new(Duration::from_millis(200), Duration::from_millis(400))
    )]
    pub election_timeout_range: DurationRange,
    #[arg(long, default_value_t = 0)]
    pub seed: u32,
}
