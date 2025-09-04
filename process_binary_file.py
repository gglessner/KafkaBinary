"""
Process Binary Kafka Data from Files

This script demonstrates how to read binary Kafka protocol data from files
and decode it using the kafka_protocol_decoder library.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0
"""

import sys
import os
from kafka_protocol_decoder import (
    KafkaProtocolDecoder,
    KafkaMessageDecoder,
    decode_kafka_message,
    is_kafka_binary,
    KafkaProtocolError
)


def read_binary_file(file_path):
    """Read binary data from a file"""
    try:
        with open(file_path, 'rb') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        return None


def process_single_message(data, offset=0):
    """Process a single Kafka message from binary data starting at offset"""
    if len(data) < offset + 4:
        raise KafkaProtocolError("Not enough data for message length")
    
    # Extract message starting at offset
    message_data = data[offset:]
    
    try:
        decoded = decode_kafka_message(message_data)
        return decoded
    except KafkaProtocolError as e:
        raise KafkaProtocolError(f"Failed to decode message at offset {offset}: {e}")


def process_multiple_messages(data):
    """Process multiple consecutive Kafka messages from binary data"""
    messages = []
    offset = 0
    
    while offset < len(data):
        try:
            # Check if we have at least 4 bytes for length
            if len(data) < offset + 4:
                print(f"Warning: Insufficient data at offset {offset} (need 4 bytes for length)")
                break
            
            # Read message length
            import struct
            message_length = struct.unpack('>I', data[offset:offset+4])[0]
            
            # Check if we have the complete message
            total_message_size = 4 + message_length
            if len(data) < offset + total_message_size:
                print(f"Warning: Incomplete message at offset {offset} "
                      f"(need {total_message_size} bytes, have {len(data) - offset})")
                break
            
            # Extract and decode this message
            message_data = data[offset:offset + total_message_size]
            decoded = decode_kafka_message(message_data)
            
            messages.append({
                'offset': offset,
                'length': message_length,
                'decoded': decoded
            })
            
            # Move to next message
            offset += total_message_size
            
        except Exception as e:
            print(f"Error processing message at offset {offset}: {e}")
            # Try to skip to next potential message (this is a heuristic)
            offset += 1
            
    return messages


def analyze_binary_file(file_path, mode='single'):
    """
    Analyze a binary file containing Kafka protocol data
    
    Args:
        file_path: Path to the binary file
        mode: 'single' for one message, 'multiple' for multiple messages
    """
    print(f"Analyzing Kafka binary file: {file_path}")
    print(f"Mode: {mode}")
    print("=" * 50)
    
    # Read the binary data
    data = read_binary_file(file_path)
    if data is None:
        return
    
    print(f"File size: {len(data)} bytes")
    print(f"First 32 bytes (hex): {data[:32].hex()}")
    
    # Check if data appears to be Kafka protocol
    detection_result = is_kafka_binary(data)
    print(f"\nKafka Detection:")
    print(f"  Is Kafka: {detection_result['is_kafka']}")
    print(f"  Confidence: {detection_result['confidence']:.2f}")
    if detection_result['detected_api_key'] is not None:
        print(f"  Detected API Key: {detection_result['detected_api_key']}")
    if detection_result['detected_version'] is not None:
        print(f"  Detected API Version: {detection_result['detected_version']}")
    
    if not detection_result['is_kafka']:
        print(f"  Warning: Data may not be Kafka protocol!")
        print(f"  Reasons: {', '.join(detection_result['reasons'][:2])}")
    print()
    
    try:
        if mode == 'single':
            # Process as a single message
            result = process_single_message(data)
            print("Decoded message:")
            print_message_details(result)
            
        elif mode == 'multiple':
            # Process as multiple messages
            messages = process_multiple_messages(data)
            print(f"Found {len(messages)} messages:")
            print()
            
            for i, msg in enumerate(messages):
                print(f"Message {i+1}:")
                print(f"  Offset: {msg['offset']}")
                print(f"  Length: {msg['length']} bytes")
                print(f"  Type: {msg['decoded']['message_type']}")
                print(f"  API Key: {msg['decoded']['api_key']}")
                print()
                
        else:
            print(f"Unknown mode: {mode}")
            
    except Exception as e:
        print(f"Error: {e}")


def print_message_details(decoded_message):
    """Print detailed information about a decoded message"""
    print(f"Message Type: {decoded_message['message_type']}")
    print(f"API Key: {decoded_message['api_key']}")
    print()
    
    if 'data' in decoded_message and 'header' in decoded_message['data']:
        header = decoded_message['data']['header']
        print("Header:")
        for key, value in header.items():
            print(f"  {key}: {value}")
        print()
    
    # Print full structure for detailed analysis
    print("Full decoded structure:")
    import json
    print(json.dumps(decoded_message, indent=2, default=str))


def create_sample_binary_file():
    """Create a sample binary file for testing"""
    from kafka_protocol_decoder import ApiKey
    import struct
    
    # Create a sample ApiVersions request
    api_key = ApiKey.API_VERSIONS
    api_version = 0
    correlation_id = 12345
    
    # Build message body
    message_body = struct.pack('>HHI', api_key, api_version, correlation_id)
    
    # Add length prefix
    message_length = len(message_body)
    complete_message = struct.pack('>I', message_length) + message_body
    
    # Write to file
    sample_file = "sample_kafka_message.bin"
    with open(sample_file, 'wb') as f:
        f.write(complete_message)
    
    print(f"Created sample binary file: {sample_file}")
    print(f"File size: {len(complete_message)} bytes")
    print(f"Content (hex): {complete_message.hex()}")
    return sample_file


def hex_dump(data, bytes_per_line=16):
    """Create a hex dump of binary data for analysis"""
    print("Hex dump:")
    for i in range(0, len(data), bytes_per_line):
        chunk = data[i:i + bytes_per_line]
        hex_part = ' '.join(f'{b:02x}' for b in chunk)
        ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in chunk)
        print(f"{i:08x}: {hex_part:<48} |{ascii_part}|")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python process_binary_file.py <file_path> [mode]")
        print()
        print("Modes:")
        print("  single   - Process file as one Kafka message (default)")
        print("  multiple - Process file as multiple consecutive messages")
        print("  sample   - Create a sample binary file for testing")
        print()
        print("Examples:")
        print("  python process_binary_file.py kafka_capture.bin")
        print("  python process_binary_file.py kafka_dump.bin multiple")
        print("  python process_binary_file.py sample")
        sys.exit(1)
    
    if sys.argv[1] == "sample":
        # Create sample file
        sample_file = create_sample_binary_file()
        print()
        print("Now you can test with:")
        print(f"  python process_binary_file.py {sample_file}")
    else:
        # Process existing file
        file_path = sys.argv[1]
        mode = sys.argv[2] if len(sys.argv) > 2 else 'single'
        
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            sys.exit(1)
        
        analyze_binary_file(file_path, mode)
