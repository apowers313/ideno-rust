// jupyter kernelspec install --name=rusty kernelspec

use serde::{Deserialize, Serialize};
// use serde_json::Result;
use std::env;
use std::error::Error;
use std::fs;
use tokio::join;
use zeromq::prelude::*;

// https://github.com/zeromq/zmq.rs/blob/c3f4eb5f4420ac06e76b2e3896e1995a80d1f08d/examples/message_broker.rs
// zeromq::DealerSocket::new();
// zeromq::RepSocket::new();

#[derive(Serialize, Deserialize)]
struct ConnectionSpec {
    ip: String,
    transport: String,
    control_port: u32,
    shell_port: u32,
    stdin_port: u32,
    hb_port: u32,
    iopub_port: u32,
    signature_scheme: String,
    key: String,
}

#[tokio::main]
async fn main() {
    // get command line args
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    if args.len() < 2 {
        println!("Wrong number of arguments, expected connection file path to be passed in as an argument.");
        std::process::exit(1);
    }

    let conn_spec: ConnectionSpec = read_conn_spec(&args[1]);

    let (_first, _second) = join!(
        create_zmq_dealer(
            String::from("shell"),
            &conn_spec.transport,
            &conn_spec.ip,
            &conn_spec.shell_port,
        ),
        create_zmq_dealer(
            String::from("control"),
            &conn_spec.transport,
            &conn_spec.ip,
            &conn_spec.control_port,
        )
    );
    // let shell_sock = create_zmq_dealer(shell_conn_str);

    // loop {
    //     let mut repl: String = shell_sock.recv().await?.try_into()?;
    //     dbg!(&repl);
    //     repl.push_str(" Reply");
    //     shell_sock.send(repl.into()).await?;
    // }
}

fn read_conn_spec(path: &String) -> ConnectionSpec {
    println!("Connection file path: {}", path);

    // open file, read contents to string
    let conn_file_contents =
        fs::read_to_string(path).expect("Something went wrong reading the file");
    println!("Connection file:\n{}", conn_file_contents);

    // convert contents to JSON
    let conn_spec: ConnectionSpec = serde_json::from_str(&conn_file_contents).unwrap();

    return conn_spec;
}

// async fn create_zmq_dealer(conn_str: String) -> Result<zeromq::DealerSocket, Box<dyn Error>> {

async fn create_zmq_dealer(
    name: String,
    transport: &String,
    ip: &String,
    port: &u32,
) -> Result<(), Box<dyn Error>> {
    let conn_str: String = format!("{}://{}:{}", transport, ip, port,);
    println!("{} connection string: {}", name, conn_str);

    let mut shell_sock = zeromq::DealerSocket::new();
    shell_sock.monitor();
    shell_sock.bind(&conn_str).await?;

    loop {
        let mut repl: String = shell_sock.recv().await?.try_into()?;
        dbg!(&repl);
        repl.push_str(" Reply");
        shell_sock.send(repl.into()).await?;
    }
}

// TODO: connect zmq to ports in JSON file
// TODO: parse packet
