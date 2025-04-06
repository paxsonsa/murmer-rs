use super::*;

#[derive(Debug, Serialize, Deserialize)]
struct TestMessage {
    value: String,
}

#[test]
fn test_wire_frame_encode_decode() {
    // Create a system message
    let node_id = Id::new();
    let msg = TestMessage {
        value: "test".to_string(),
    };

    // Create a wire frame
    let frame = Frame::ok(node_id.clone(), None, msg);

    // Encode the frame
    let encoded = frame.encode().unwrap();

    // Verify the length prefix
    let mut prefix_bytes = encoded.slice(0..8);
    assert_eq!(prefix_bytes.get_u64(), (encoded.len() - 8) as u64);

    // Decode the frame
    let body_bytes = encoded.slice(8..encoded.len());
    let decoded = Frame::<TestMessage>::decode(body_bytes).unwrap();

    // Verify the decoded frame
    match &decoded.payload {
        Payload::Ok(msg) => assert_eq!(msg.value, "test"),
        _ => panic!("Decoded wrong message type"),
    }
}

#[test]
fn test_frame_reader() {
    // Create a few frames
    let node_id = Id::new();

    let frame1 = Frame::ok(
        node_id.clone(),
        None,
        TestMessage {
            value: "test1".to_string(),
        },
    );

    let frame2 = Frame::ok(
        node_id.clone(),
        None,
        TestMessage {
            value: "test2".to_string(),
        },
    );

    // Encode the frames
    let encoded1 = frame1.encode().unwrap();
    let encoded2 = frame2.encode().unwrap();

    println!("Encoded frame 1: {:?}", encoded1);
    println!("Encoded frame 2: {:?}", encoded2);

    // Create a reader
    let mut reader = FrameParser::<TestMessage>::new();

    // Add partial data and verify no complete frame yet
    reader.extend(&encoded1[0..4]);
    assert!(reader.parse().unwrap().is_none());

    // Add the rest of the first frame
    reader.extend(&encoded1[4..]);
    let parsed1 = reader.parse().unwrap().unwrap();

    // Verify no more frames
    assert!(reader.parse().unwrap().is_none());

    // Add the second frame
    reader.extend(&encoded2);
    let parsed2 = reader.parse().unwrap().unwrap();

    // Verify no more frames
    assert!(reader.parse().unwrap().is_none());

    // Verify the parsed frames
    match &parsed1.payload {
        Payload::Ok(msg) => {
            assert_eq!(msg.value, "test1");
        }
        _ => panic!("Parsed wrong message type for frame 1"),
    }

    match &parsed2.payload {
        Payload::Ok(msg) => {
            assert_eq!(msg.value, "test2");
        }
        _ => panic!("Parsed wrong message type for frame 2"),
    }
}
