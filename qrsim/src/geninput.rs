use std::time::Duration;

use clap::Clap;
use eyre::{eyre, WrapErr};
use humantime::parse_duration;
use qrsim::{BrokerEvent, Event, QueryId, QueryRequest, RequestId, TimedEvent};
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rand_distr::{Distribution, Poisson, Uniform};

struct DurationArg(Duration);

impl std::str::FromStr for DurationArg {
    type Err = eyre::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).wrap_err("invalid time").map(DurationArg)
    }
}

#[derive(strum::EnumString)]
#[strum(serialize_all = "lowercase")]
enum Format {
    Json,
    MsgPack,
}

/// Generates input for distributed search simulation.
#[derive(Clap)]
#[clap(version, author)]
struct Opts {
    /// Expected number of queries per second.
    #[clap(long)]
    queries_per_second: f64,

    /// Number of queries to draw from.
    #[clap(long)]
    num_queries: usize,

    /// Time of intended simulation in microseconds.
    #[clap(short, long)]
    time: DurationArg,

    /// Seed to use for random number generator.
    #[clap(short, long)]
    seed: Option<u64>,

    /// Output format.
    #[clap(short, long, possible_values = &["json", "msgpack"], default_value = "msgpack")]
    format: Format,
}

fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let opts: Opts = Opts::parse();

    let micro_distr = Uniform::new(0, 1000);
    let query_id_distr = Uniform::new(0, opts.num_queries);
    let milli_distr = Poisson::new(opts.queries_per_second / 1000.0)
        .map_err(|_| eyre!("invalid lambda: {}", opts.queries_per_second))?;

    let mut rng = if let Some(seed) = opts.seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_entropy()
    };

    let mut ids = 0_usize..;

    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    let mut events = Vec::<TimedEvent>::new();
    for milli in 0..(opts.time.0.as_micros() / 1000) as u64 {
        let num_requests: f64 = milli_distr.sample(&mut rng);
        for _ in 0..(num_requests as usize) {
            let query_id = QueryId::from(query_id_distr.sample(&mut rng));
            let micro = micro_distr.sample(&mut rng);
            let time = Duration::from_micros(micro + 1000 * milli);
            let request_id = RequestId::from(ids.next().expect("value from infinite range"));
            let request = QueryRequest::new(request_id, query_id, time);
            let event = TimedEvent {
                event: Event::Broker(BrokerEvent::NewRequest(request)),
                time,
            };
            events.push(event);
        }
    }
    match opts.format {
        Format::Json => {
            serde_json::to_writer(&mut writer, &events)?;
        }
        Format::MsgPack => {
            rmp_serde::encode::write(&mut writer, &events)?;
        }
    }

    Ok(())
}
