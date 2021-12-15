// jupyter kernelspec install --name=rusty kernelspec

use serde::{Deserialize, Serialize};
// use serde_json::Value;
use data_encoding::HEXLOWER;
use ring::hmac;
use std::env;
use std::error::Error;
use std::fs;
use tokio::join;
use zeromq::prelude::*;
use zeromq::ZmqMessage;

#[derive(Debug, Serialize, Deserialize)]
struct MessageHeader {
    msg_id: String,
    session: String,
    username: String,
    date: String,
    msg_type: String,
    version: String,
}

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

    dbg!(header);
    let header_str = std::str::from_utf8(&header).unwrap();
    let header_value: MessageHeader = serde_json::from_str(header_str).unwrap();
    dbg!(&header_value);
    // validate_header(&header_value)?;

    Ok(())
}

#[allow(dead_code)]
fn hmac_sign(zmq: &ZmqMessage, key: hmac::Key) -> String {
    let mut hmac_ctx = hmac::Context::with_key(&key);
    hmac_ctx.update(zmq.get(2).unwrap()); // header
    hmac_ctx.update(zmq.get(3).unwrap()); // parent header
    hmac_ctx.update(zmq.get(4).unwrap()); // metadata
    hmac_ctx.update(zmq.get(5).unwrap()); // content
    let tag = hmac_ctx.sign();
    dbg!(tag);
    let sig = HEXLOWER.encode(tag.as_ref());

    return sig;
}

#[allow(dead_code)]
fn hmac_verify(
    zmq: &ZmqMessage,
    key: hmac::Key,
    sig: String,
) -> Result<(), ring::error::Unspecified> {
    dbg!(&zmq);
    let mut msg = Vec::<u8>::new();
    msg.extend(zmq.get(2).unwrap()); // header
    msg.extend(zmq.get(3).unwrap()); // parent header
    msg.extend(zmq.get(4).unwrap()); // metadata
    msg.extend(zmq.get(5).unwrap()); // content
    hmac::verify(&key, &msg.as_ref(), sig.as_ref())?;

    Ok(())
}

#[test]
fn hmac_verify_test() {
    let key_value = "1f5cec86-8eaa942eef7f5a35b51ddcf5";
    let key = hmac::Key::new(hmac::HMAC_SHA256, key_value.as_ref());

    let delim = "<IDS|MSG>".as_bytes().to_vec();
    let hash = "43a5c45062e0b6bcc59c727f90165ad1d2eb02e1c5317aa25c2c2049d96d3b6a"
        .as_bytes()
        .to_vec();
    let header = "{\"msg_id\":\"c0fd20872c1b4d1c87e7fc814b75c93e_0\",\"msg_type\":\"kernel_info_request\",\"username\":\"ampower\",\"session\":\"c0fd20872c1b4d1c87e7fc814b75c93e\",\"date\":\"2021-12-10T06:20:40.259695Z\",\"version\":\"5.3\"}".as_bytes().to_vec();
    let parent_header = "{}".as_bytes().to_vec();
    let metadata = "{}".as_bytes().to_vec();
    let content = "{}".as_bytes().to_vec();

    let mut test_msg = ZmqMessage::from(delim);
    test_msg.push_back(hash.into());
    test_msg.push_back(header.into());
    test_msg.push_back(parent_header.into());
    test_msg.push_back(metadata.into());
    test_msg.push_back(content.into());

    dbg!(test_msg.clone());
    match hmac_verify(
        &test_msg,
        key,
        String::from("43a5c45062e0b6bcc59c727f90165ad1d2eb02e1c5317aa25c2c2049d96d3b6a"),
    ) {
        Ok(_) => assert!(true),
        Err(_) => assert!(false, "signature validation failed"),
    }
}

#[test]
fn hmac_sign_test() {
    let key_value = "1f5cec86-8eaa942eef7f5a35b51ddcf5";
    let key = hmac::Key::new(hmac::HMAC_SHA256, key_value.as_ref());

    let delim = "<IDS|MSG>";
    let hash = "43a5c45062e0b6bcc59c727f90165ad1d2eb02e1c5317aa25c2c2049d96d3b6a"
        .as_bytes()
        .to_vec();
    let header = "{\"msg_id\":\"c0fd20872c1b4d1c87e7fc814b75c93e_0\",\"msg_type\":\"kernel_info_request\",\"username\":\"ampower\",\"session\":\"c0fd20872c1b4d1c87e7fc814b75c93e\",\"date\":\"2021-12-10T06:20:40.259695Z\",\"version\":\"5.3\"}".as_bytes().to_vec();
    let parent_header = "{}".as_bytes().to_vec();
    let metadata = "{}".as_bytes().to_vec();
    let content = "{}".as_bytes().to_vec();

    let mut test_msg = ZmqMessage::from(delim);
    test_msg.push_back(hash.into());
    test_msg.push_back(header.into());
    test_msg.push_back(parent_header.into());
    test_msg.push_back(metadata.into());
    test_msg.push_back(content.into());

    dbg!(test_msg.clone());
    let sig = hmac_sign(&test_msg, key);
    println!("Signature: {}", sig);
    assert_eq!(
        sig,
        "43a5c45062e0b6bcc59c727f90165ad1d2eb02e1c5317aa25c2c2049d96d3b6a"
    );
}

#[test]
fn send_test() {
    let now = std::time::SystemTime::now();
    let now: chrono::DateTime<chrono::Utc> = now.into();
    let now = now.to_rfc3339();
    #[allow(unused_variables)]
    let header_struct = MessageHeader {
        msg_id: uuid::Uuid::new_v4().to_string(),
        session: uuid::Uuid::new_v4().to_string(),
        // FIXME:
        username: "<TODO>".to_string(),
        date: now.to_string(),
        msg_type: "status".to_string(),
        // TODO: this should be taken from a global,
        version: "5.3".to_string(),
    };
}
