"""
Example usage of the Kafka Protocol Decoder

This demonstrates how to use the low-level Kafka binary protocol decoder
to parse actual Kafka wire protocol messages.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0
"""

from kafka_protocol_decoder import (
    KafkaProtocolDecoder, 
    KafkaMessageDecoder, 
    decode_kafka_message,
    is_kafka_binary,
    ApiKey,
    ErrorCode
)
import struct


def create_sample_api_versions_request():
    """Create a sample ApiVersions request for testing"""
    
    # Build the request manually
    api_key = ApiKey.API_VERSIONS  # 18
    api_version = 0
    correlation_id = 1
    
    # Pack the header
    header = struct.pack('>HHI', api_key, api_version, correlation_id)
    
    # ApiVersions v0 has no body
    message_body = header
    
    # Add message length prefix
    message_length = len(message_body)
    complete_message = struct.pack('>I', message_length) + message_body
    
    return complete_message


def create_sample_metadata_request():
    """Create a sample Metadata request for testing"""
    
    api_key = ApiKey.METADATA  # 3
    api_version = 1
    correlation_id = 2
    client_id = "test-client"
    
    # Pack header with client_id (v1+)
    client_id_bytes = client_id.encode('utf-8')
    header = struct.pack('>HHIH', api_key, api_version, correlation_id, len(client_id_bytes))
    header += client_id_bytes
    
    # Add topics array (empty array = -1 means all topics)
    topics_array = struct.pack('>i', -1)  # Empty array for "all topics"
    
    message_body = header + topics_array
    message_length = len(message_body)
    complete_message = struct.pack('>I', message_length) + message_body
    
    return complete_message


def demonstrate_primitive_types():
    """Demonstrate decoding of primitive types"""
    print("=== Primitive Types Demonstration ===")
    
    # Create test data with various primitive types
    test_data = b''
    
    # INT8, INT16, INT32, INT64
    test_data += struct.pack('>bhiq', -42, 1234, -567890, 9876543210)
    
    # String (length-prefixed)
    test_string = "Hello Kafka"
    test_data += struct.pack('>H', len(test_string)) + test_string.encode('utf-8')
    
    # Bytes (length-prefixed)
    test_bytes = b'\x01\x02\x03\x04'
    test_data += struct.pack('>I', len(test_bytes)) + test_bytes
    
    # Create decoder
    decoder = KafkaProtocolDecoder(test_data)
    
    # Decode primitives
    print(f"INT8: {decoder.read_int8()}")
    print(f"INT16: {decoder.read_int16()}")
    print(f"INT32: {decoder.read_int32()}")
    print(f"INT64: {decoder.read_int64()}")
    print(f"STRING: '{decoder.read_string()}'")
    print(f"BYTES: {decoder.read_bytes()}")
    print()


def demonstrate_message_decoding():
    """Demonstrate decoding of complete Kafka messages"""
    print("=== Message Decoding Demonstration ===")
    
    # Test ApiVersions request
    print("1. ApiVersions Request:")
    api_versions_msg = create_sample_api_versions_request()
    print(f"   Raw bytes: {api_versions_msg.hex()}")
    
    try:
        decoded = decode_kafka_message(api_versions_msg)
        print(f"   Decoded: {decoded}")
    except Exception as e:
        print(f"   Error: {e}")
    print()
    
    # Test Metadata request
    print("2. Metadata Request:")
    metadata_msg = create_sample_metadata_request()
    print(f"   Raw bytes: {metadata_msg.hex()}")
    
    try:
        decoded = decode_kafka_message(metadata_msg)
        print(f"   Decoded: {decoded}")
    except Exception as e:
        print(f"   Error: {e}")
    print()


def demonstrate_kafka_detection():
    """Demonstrate Kafka binary detection"""
    print("=== Kafka Binary Detection Demonstration ===")
    
    test_cases = [
        (create_sample_api_versions_request(), "Valid Kafka ApiVersions"),
        (b'\xFF\xFE\xFD\xFC\xFB\xFA\xF9\xF8', "Random binary data"),
        (b'GET / HTTP/1.1\r\n\r\n', "HTTP request"),
        (b'{"key": "value"}', "JSON data"),
    ]
    
    for data, description in test_cases:
        print(f"Testing: {description}")
        print(f"   Data: {data[:16].hex()}{'...' if len(data) > 16 else ''}")
        
        result = is_kafka_binary(data)
        print(f"   Is Kafka: {result['is_kafka']}")
        print(f"   Confidence: {result['confidence']:.2f}")
        if result['detected_api_key'] is not None:
            print(f"   API Key: {result['detected_api_key']}")
        print(f"   Reasons: {', '.join(result['reasons'][:2])}")
        print()


def demonstrate_error_handling():
    """Demonstrate error handling for malformed data"""
    print("=== Error Handling Demonstration ===")
    
    test_cases = [
        (b'', "Empty data"),
        (b'\x00\x00\x00\x05\x00', "Incomplete message"),
        (b'\x00\x00\x00\x04\xFF\xFF\xFF\xFF', "Invalid data"),
    ]
    
    for data, description in test_cases:
        print(f"Testing: {description}")
        print(f"   Data: {data.hex()}")
        try:
            result = decode_kafka_message(data)
            print(f"   Result: {result}")
        except Exception as e:
            print(f"   Expected error: {e}")
        print()


def analyze_real_kafka_capture():
    """
    Template for analyzing real Kafka network captures
    
    To use this:
    1. Capture Kafka traffic with tcpdump or Wireshark
    2. Extract the TCP payload bytes
    3. Pass them to this function
    """
    print("=== Real Kafka Capture Analysis ===")
    print("To analyze real Kafka traffic:")
    print("1. Use tcpdump: tcpdump -i any -s 0 -w kafka.pcap port 9092")
    print("2. Extract TCP payload with Wireshark or tshark")
    print("3. Feed the bytes to decode_kafka_message()")
    print()
    
    # Example with hypothetical captured data
    # Replace this with actual captured bytes
    sample_capture = b'\x00\x00\x00\x08\x00\x12\x00\x00\x00\x00\x00\x01'
    
    print(f"Sample captured bytes: {sample_capture.hex()}")
    try:
        decoded = decode_kafka_message(sample_capture)
        print(f"Decoded structure: {decoded}")
    except Exception as e:
        print(f"Decode error: {e}")


if __name__ == "__main__":
    print("Kafka Protocol Decoder - Example Usage")
    print("=" * 50)
    print()
    
    demonstrate_primitive_types()
    demonstrate_message_decoding()
    demonstrate_kafka_detection()
    demonstrate_error_handling()
    analyze_real_kafka_capture()
    
    print("=" * 50)
    print("For more advanced usage:")
    print("- Extend KafkaMessageDecoder with additional message types")
    print("- Add support for specific API versions you need")
    print("- Implement record batch decompression for FETCH responses")
    print("- Add support for SASL authentication frames")
