// jupyter kernelspec install --name=rusty kernelspec

use serde::{Deserialize, Serialize};
// use serde_json::Value;
use std::env;
use std::error::Error;
use std::fs;
use tokio::join;
use zeromq::prelude::*;
use zeromq::ZmqMessage;

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

    // create connection strings from connection file
    let conn_spec: ConnectionSpec = read_conn_spec(&args[1]);
    let shell_conn_str =
        create_conn_str(&conn_spec.transport, &conn_spec.ip, &conn_spec.shell_port);
    let control_conn_str =
        create_conn_str(&conn_spec.transport, &conn_spec.ip, &conn_spec.control_port);
    let iopub_conn_str =
        create_conn_str(&conn_spec.transport, &conn_spec.ip, &conn_spec.iopub_port);
    let stdin_conn_str =
        create_conn_str(&conn_spec.transport, &conn_spec.ip, &conn_spec.stdin_port);
    let hb_conn_str = create_conn_str(&conn_spec.transport, &conn_spec.ip, &conn_spec.hb_port);

    // create zmq sockets
    let (_first, _second, _third, _fourth, _fifth) = join!(
        create_zmq_dealer(String::from("shell"), &shell_conn_str),
        create_zmq_dealer(String::from("control"), &control_conn_str),
        create_zmq_publisher(String::from("iopub"), &iopub_conn_str),
        create_zmq_dealer(String::from("stdin"), &stdin_conn_str),
        create_zmq_reply(String::from("hb"), &hb_conn_str),
    );
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

fn create_conn_str(transport: &String, ip: &String, port: &u32) -> String {
    let conn_str: String = format!("{}://{}:{}", transport, ip, port,);
    return conn_str;
}

async fn create_zmq_dealer(name: String, conn_str: &String) -> Result<(), Box<dyn Error>> {
    println!("dealer '{}' connection string: {}", name, conn_str);

    let mut sock = zeromq::DealerSocket::new();
    sock.monitor();
    sock.bind(&conn_str).await?;

    // TODO(apowers313) pop this out into it's own function: we need to send on sock, as well as receive
    loop {
        let data = sock.recv().await?;
        dbg!(&data);
        println!("{} got packet!", name);
        println!("size is: {}", data.len());
        parse_zmq_packet(&data)?;
    }
}

async fn create_zmq_publisher(
    name: String,
    conn_str: &String,
) -> Result<zeromq::PubSocket, Box<dyn Error>> {
    println!("iopub {} connection string: {}", name, conn_str);

    let mut sock = zeromq::PubSocket::new();
    sock.bind(&conn_str).await?;

    // no loop, only used for broadcasting status to Jupyter frontends

    Ok(sock)
}

async fn create_zmq_reply(name: String, conn_str: &String) -> Result<(), Box<dyn Error>> {
    println!("reply '{}' connection string: {}", name, conn_str);

    let mut sock = zeromq::RepSocket::new(); // TODO(apowers313) exact same as dealer, refactor
    sock.monitor();
    sock.bind(&conn_str).await?;

    loop {
        let msg = sock.recv().await?;
        dbg!(&msg);
        println!("{} got packet!", name);
    }
}

fn parse_zmq_packet(data: &ZmqMessage) -> Result<(), Box<dyn Error>> {
    let _delim = data.get(0);
    let _hmac = data.get(1);
    let header = data.get(2).unwrap();
    let _parent_header = data.get(3);
    let _metadata = data.get(4);
    let _content = data.get(5);

    println!("header:");
    dbg!(header);
    let header_str = std::str::from_utf8(&header).unwrap();
    let header_value: MessageHeader = serde_json::from_str(header_str).unwrap();
    println!("header_value");
    dbg!(&header_value);
    // validate_header(&header_value)?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageHeader {
    msg_id: String,
    session: String,
    username: String,
    date: String,
    msg_type: String,
    version: String,
}

// fn validate_header(header_value: &Value) -> Result<MessageHeader, Box<dyn Error>> {
//     dbg!(header_value);
//     let msg_id = header_value["msg_id"].as_str().ok_or("bad msg_id")?;
//     let session = header_value["session"].as_str().ok_or("bad msg_id")?;
//     let username = header_value["username"].as_str().ok_or("bad msg_id")?;
//     let date = header_value["date"].as_str().ok_or("bad msg_id")?;
//     let msg_type = header_value["msg_type"].as_str().ok_or("bad msg_id")?;
//     let version = header_value["version"].as_str().ok_or("bad msg_id")?;

//     let parsed_header = MessageHeader {
//         msg_id: String::from(msg_id),
//     };
//     Ok(parsed_header)
// }
