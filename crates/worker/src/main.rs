use clap::Parser;
use redis::Commands;
use taskforge_core::TaskSpec;

#[derive(Parser, Debug)]
#[command(name = "taskforge-worker")]
#[command(about = "Taskforge worker (Redis Streams)", long_about = None)]
struct Cli {
    #[arg(long)]
    broker_url: String,
    #[arg(long, default_value = "taskforge.tasks")]
    stream: String,
    #[arg(long, default_value = "$")]
    last_id: String,
    #[arg(long, default_value = "5000")]
    block_ms: u64,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut conn = redis::Client::open(cli.broker_url)?.get_connection()?;
    let mut last_id = cli.last_id;

    loop {
        let reply: redis::Value = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(cli.block_ms)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cli.stream)
            .arg(&last_id)
            .query(&mut conn)?;

        if let Some((id, payload)) = extract_payload(reply) {
            last_id = id.clone();
            let task: TaskSpec = serde_json::from_str(&payload)
                .map_err(|e| anyhow::anyhow!("Invalid task payload JSON: {e}"))?;
            println!("Received task {} name={}", task.id, task.name);
        }
    }
}

fn extract_payload(reply: redis::Value) -> Option<(String, String)> {
    let streams = match reply {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let first_stream = streams.get(0)?;
    let stream_items = match first_stream {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    if stream_items.len() < 2 {
        return None;
    }

    let entries = match &stream_items[1] {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let first_entry = entries.get(0)?;
    let entry_items = match first_entry {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    if entry_items.len() < 2 {
        return None;
    }

    let id = match &entry_items[0] {
        redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
        _ => return None,
    };

    let fields = match &entry_items[1] {
        redis::Value::Bulk(items) => items,
        _ => return None,
    };

    let mut i = 0;
    while i + 1 < fields.len() {
        let key = match &fields[i] {
            redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => return None,
        };
        let value = match &fields[i + 1] {
            redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => return None,
        };
        if key == "payload" {
            return Some((id, value));
        }
        i += 2;
    }

    None
}
