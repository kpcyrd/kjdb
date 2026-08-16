use kjdb::Database;
use kjdb::errors::*;
use kjdb::futures::{Stream, TryStreamExt};
use std::env;
use std::io::IsTerminal;
use std::ops::Bound;
use tokio::io::{self, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

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

async fn write_iter<W: AsyncWrite + Unpin>(
    mut w: W,
    iter: impl Iterator<Item = &str>,
) -> Result<()> {
    for key in iter {
        w.write_all(key.as_bytes()).await?;
        w.write_all(b"\n").await?;
    }
    Ok(())
}

async fn write_tuple_stream<W: AsyncWrite + Unpin>(
    mut w: W,
    mut stream: impl Stream<Item = Result<(&str, String)>> + Unpin,
) -> Result<()> {
    while let Some((key, value)) = stream.try_next().await? {
        w.write_all(key.as_bytes()).await?;
        w.write_all(b"\n").await?;
        w.write_all(value.as_bytes()).await?;
        w.write_all(b"\n").await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(path) = env::args().nth(1) else {
        eprintln!("Usage: shell <path>");
        std::process::exit(1);
    };

    let mut db = Database::<String>::open_writer(&path).await?;

    let interactive = std::io::stdin().is_terminal();

    let stdin = BufReader::new(io::stdin());
    let mut stdout = io::stdout();
    let mut lines = stdin.lines();

    loop {
        if interactive {
            stdout.write_all(b"> ").await?;
            stdout.flush().await?;
        }

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
            "put" => {
                let Some((key, value)) = args.split_once(' ') else {
                    eprintln!("Usage: put <key> <value>");
                    continue;
                };

                db.put(key.to_string(), &value.to_string()).await?;
            }
            "delete" => {
                for key in args.split_whitespace() {
                    db.delete(key).await?;
                }
            }
            "range_keys" => {
                let Some(range) = range(args) else {
                    eprintln!("Usage: range_keys ..");
                    eprintln!("Usage: range_keys <start>..");
                    eprintln!("Usage: range_keys ..<end>");
                    eprintln!("Usage: range_keys <start>..<end>");
                    eprintln!("Usage: range_keys <start>..=<end>");
                    continue;
                };

                write_iter(&mut stdout, db.range_keys(range)).await?;
            }
            "range_items" => {
                let Some(range) = range(args) else {
                    eprintln!("Usage: range_items ..");
                    eprintln!("Usage: range_items <start>..");
                    eprintln!("Usage: range_items ..<end>");
                    eprintln!("Usage: range_items <start>..<end>");
                    eprintln!("Usage: range_items <start>..=<end>");
                    continue;
                };

                let iter = db.range_items(range);
                tokio::pin!(iter);
                write_tuple_stream(&mut stdout, iter).await?;
            }
            "prefix_keys" => {
                write_iter(&mut stdout, db.prefix_keys(args)).await?;
            }
            "prefix_items" => {
                let iter = db.prefix_items(args);
                tokio::pin!(iter);
                write_tuple_stream(&mut stdout, iter).await?;
            }
            unknown => {
                eprintln!("Unrecognized command: {unknown:?}");
            }
        }
    }

    if interactive {
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }

    Ok(())
}
