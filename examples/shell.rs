use kjdb::Database;
use kjdb::errors::*;
use kjdb::futures::TryStreamExt;
use std::env;
use std::ops::Bound;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

fn range(args: &str) -> Option<(Bound<&str>, Bound<&str>)> {
    let (start, end) = args.split_once("..")?;

    Some(match (start, end, end.strip_prefix('=')) {
        ("", "", _) => (Bound::Unbounded, Bound::Unbounded),
        (s, "", _) => (Bound::Included(s), Bound::Unbounded),
        ("", e, None) => (Bound::Unbounded, Bound::Excluded(e)),
        ("", _, Some(e)) => (Bound::Unbounded, Bound::Included(e)),
        (s, e, None) => (Bound::Included(s), Bound::Excluded(e)),
        (s, _, Some(e)) => (Bound::Included(s), Bound::Included(e)),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(path) = env::args().nth(1) else {
        eprintln!("Usage: shell <path>");
        std::process::exit(1);
    };

    let mut db = Database::<String>::open_writer(&path).await?;

    let stdin = BufReader::new(io::stdin());
    let mut stdout = io::stdout();
    let mut lines = stdin.lines();

    loop {
        stdout.write_all(b"> ").await?;
        stdout.flush().await?;

        let Some(line) = lines.next_line().await? else {
            break;
        };
        let (op, args) = line.split_once(' ').unwrap_or((&line, ""));

        match op {
            "get" => {
                for key in args.split_whitespace() {
                    if let Some(value) = db.get(key).await? {
                        stdout.write_all(value.as_bytes()).await?;
                        stdout.write_all(b"\n").await?;
                    }
                }
            }
            "write" => {
                let Some((key, value)) = args.split_once(' ') else {
                    eprintln!("Usage: write <key> <value>");
                    continue;
                };

                db.write(key.to_string(), &value.to_string()).await?;
            }
            "delete" => {
                for key in args.split_whitespace() {
                    db.delete(key).await?;
                }
            }
            "keys" => {
                let Some(range) = range(args) else {
                    eprintln!("Usage: keys ..");
                    eprintln!("Usage: keys <start>..");
                    eprintln!("Usage: keys ..<end>");
                    eprintln!("Usage: keys <start>..<end>");
                    eprintln!("Usage: keys <start>..=<end>");
                    continue;
                };

                for key in db.range_keys(range) {
                    stdout.write_all(key.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                }
            }
            "items" => {
                let Some(range) = range(args) else {
                    eprintln!("Usage: items ..");
                    eprintln!("Usage: items <start>..");
                    eprintln!("Usage: items ..<end>");
                    eprintln!("Usage: items <start>..<end>");
                    eprintln!("Usage: items <start>..=<end>");
                    continue;
                };

                let iter = db.range_items(range);
                tokio::pin!(iter);
                while let Some((key, value)) = iter.try_next().await? {
                    stdout.write_all(key.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.write_all(value.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                }
            }
            _ => (),
        }
    }

    stdout.write_all(b"\n").await?;
    stdout.flush().await?;

    Ok(())
}
