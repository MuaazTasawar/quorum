//! A tour of the client against a running cluster.
//!
//!     QUORUM_NODES=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 \
//!         cargo run -p client --example smoke
//!
//! The addresses are the nodes' HTTP (`QUORUM_METRICS_ADDR`) addresses.

use client::{ClientError, QuorumClient};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let nodes: HashMap<u32, String> = std::env::var("QUORUM_NODES")
        .expect("set QUORUM_NODES=1=host:port,2=host:port,...")
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .filter_map(|(id, addr)| Some((id.trim().parse().ok()?, addr.trim().to_string())))
        .collect();
    let client = QuorumClient::new(nodes);

    // Text.
    client.put("greeting", "héllo 世界").await?;
    println!("greeting = {:?}", client.get("greeting").await?);

    // Any bytes at all, exactly as written.
    let blob: Vec<u8> = (0..=255u8).collect();
    client.put_bytes("blob", &blob).await?;
    let back = client.get_bytes("blob").await?.expect("just written");
    println!("blob     = {} bytes, identical: {}", back.len(), back == blob);

    // Keys may contain anything that is not a control character.
    client.put("path/with?odd#chars%", "ok").await?;
    println!("odd key  = {:?}", client.get("path/with?odd#chars%").await?);

    // A missing key is None; an empty value is Some("").
    println!("missing  = {:?}", client.get("no-such-key").await?);
    client.put("empty", "").await?;
    println!("empty    = {:?}", client.get("empty").await?);

    // A request that can never succeed says why, and is not retried.
    match client.put("", "x").await {
        Err(ClientError::InvalidKey(why)) => println!("bad key  : {why}"),
        other => println!("unexpected: {other:?}"),
    }

    client.delete("greeting").await?;
    println!("deleted  = {:?}", client.get("greeting").await?);
    println!("members  = {:?}", client.members().await?);
    Ok(())
}
