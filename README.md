# Kafka Binary Protocol Decoder

A low-level Python decoder for Apache Kafka's binary wire protocol, implemented according to the [official Kafka protocol specification](https://kafka.apache.org/protocol).

## Features

- **Complete Protocol Primitive Support**: All Kafka protocol primitive types (INT8, INT16, INT32, INT64, STRING, BYTES, VARINT, etc.)
- **Dual Decoder Architecture**: Separate decoders for wire protocol vs topic message content
- **Version-Aware Decoding**: Supports different API versions and protocol evolution
- **Tagged Fields Support**: Handles KIP-482 tagged fields for newer protocol versions
- **Kafka Binary Detection**: Intelligent detection of Kafka protocol data with confidence scoring
- **Multiple Input Formats**: Supports hex strings, Python bytes strings, and binary files
- **Comprehensive Error Handling**: Detailed error messages for malformed data
- **File Processing Tools**: Ready-to-use scripts for analyzing binary files
- **Interactive Analysis**: Command-line tools with smart format detection
- **Extensible Design**: Easy to add support for new message types

## Supported Protocol Elements

### Primitive Types
- **Integers**: INT8, INT16, INT32, INT64, UINT32
- **Variable Length**: VARINT (for newer protocol versions)
- **Strings**: STRING, COMPACT_STRING (with proper null handling)
- **Binary Data**: BYTES, COMPACT_BYTES
- **Collections**: ARRAY, COMPACT_ARRAY
- **Other**: UUID, TAGGED_FIELDS

### Message Structures
- **Wire Protocol**: Request/Response headers, API versioning, error codes
- **Topic Messages**: Message sets (v0/v1), record batches (v2+), keys/values
- **Content Analysis**: JSON detection, UTF-8 strings, numeric types

### Input Formats
- **Hex Strings**: `087f68656c6c6f00`
- **Python Bytes**: `b'\x08\x7fhello\x00'`, `"\x08\x7fhello\x00"`
- **Binary Files**: `.bin`, `.log`, network captures

## Installation

No external dependencies required - uses only Python standard library.

```bash
# Clone or download the files
# No pip install needed
```

## Quick Start

### Basic Message Decoding
```python
from kafka_protocol_decoder import decode_kafka_message

# Decode a complete Kafka message from network capture
raw_bytes = b'\x00\x00\x00\x08\x00\x12\x00\x00\x00\x00\x00\x01'
decoded = decode_kafka_message(raw_bytes)
print(decoded)
```

### Binary Detection
```python
from kafka_protocol_decoder import is_kafka_binary

# Check if binary data is Kafka protocol
hex_data = "000000080012000000003039"
binary_data = bytes.fromhex(hex_data)

result = is_kafka_binary(binary_data)
print(f"Is Kafka: {result['is_kafka']}")        # True
print(f"Confidence: {result['confidence']}")    # 1.00
print(f"API Key: {result['detected_api_key']}")  # 18
```

### Topic Message Decoding
```python
from kafka_message_decoder import decode_kafka_topic_data

# Decode topic message content (keys/values)
topic_data = b'\x08\x7fhello world\x00'
decoded = decode_kafka_topic_data(topic_data)
print(f"Format: {decoded['format']}")
```

### File Processing
```python
# Process binary files containing Kafka data
python process_binary_file.py kafka_capture.bin
python process_binary_file.py kafka_dump.bin multiple
```

### Format Detection
```python
# Auto-detect data type and get recommendations
python what_decoder.py "b'\x08\x7fhello\x00'"
python what_decoder.py 087f68656c6c6f00
```

## Detailed Usage

### Basic Protocol Decoding

```python
from kafka_protocol_decoder import KafkaProtocolDecoder

# Raw binary data from Kafka wire protocol
data = b'\x00\x42\x00\x00\x04\xd2'  # INT8, INT16, INT32

decoder = KafkaProtocolDecoder(data)
value1 = decoder.read_int8()    # 0
value2 = decoder.read_int16()   # 66
value3 = decoder.read_int32()   # 1234
```

### Message-Level Decoding

```python
from kafka_protocol_decoder import KafkaMessageDecoder, KafkaProtocolDecoder

decoder = KafkaProtocolDecoder(message_bytes)
message_decoder = KafkaMessageDecoder(decoder)

# Decode request header
header = message_decoder.decode_request_header(api_version=1)
print(f"API Key: {header['api_key']}")
print(f"Correlation ID: {header['correlation_id']}")
```

### Kafka Binary Detection

```python
from kafka_protocol_decoder import is_kafka_binary

# Detect Kafka protocol data with detailed analysis
result = is_kafka_binary(binary_data)

print(f"Is Kafka: {result['is_kafka']}")
print(f"Confidence: {result['confidence']:.2f}")
print(f"Detected API Key: {result['detected_api_key']}")
print(f"Detected Version: {result['detected_version']}")
print("Detection reasons:")
for reason in result['reasons']:
    print(f"  - {reason}")
```

### Handling Different Message Types

```python
# The main decode function automatically detects message types
result = decode_kafka_message(raw_bytes)

if result['message_type'] == 'ApiVersionsRequest':
    print("Received ApiVersions request")
elif result['message_type'] == 'MetadataRequest':
    print("Received Metadata request")
else:
    print(f"Unknown message type: {result['message_type']}")
```

## Protocol Compliance

This decoder implements the binary protocol as specified in the [official Kafka protocol documentation](https://kafka.apache.org/protocol):

- **Network Byte Order**: All multi-byte integers use big-endian encoding
- **Length Prefixing**: Strings and byte arrays are length-prefixed
- **Null Handling**: Proper handling of null values (-1 length)
- **API Versioning**: Support for protocol evolution across versions
- **Tagged Fields**: KIP-482 tagged fields for newer protocol versions

## Supported API Types

Currently implements decoders for:
- **ApiVersions** (Key: 18) - Protocol version negotiation
- **Metadata** (Key: 3) - Topic and broker metadata
- **Generic Request Headers** - For any API type

Easy to extend for additional message types by adding methods to `KafkaMessageDecoder`.

## Error Handling

The decoder provides detailed error messages for common issues:

```python
from kafka_protocol_decoder import KafkaProtocolError

try:
    result = decode_kafka_message(malformed_data)
except KafkaProtocolError as e:
    print(f"Protocol error: {e}")
```

Common error scenarios:
- Insufficient data for requested type
- Invalid length prefixes
- Malformed message structure
- Unknown protocol versions

## Examples

### Processing Different Input Formats

```bash
# Wire protocol analysis
python kafka_protocol_decoder.py  # Interactive mode
python process_binary_file.py protocol_capture.bin

# Topic message analysis  
python kafka_message_decoder.py topic_dump.bin
python kafka_message_decoder.py "b'\x08\x7fhello\x00'"
python kafka_message_decoder.py 087f68656c6c6f00

# Format detection and recommendations
python what_decoder.py "b'\x08\x7fhello\x00'"
python what_decoder.py explain
python what_decoder.py interactive

# Multiple message analysis
python process_binary_file.py kafka_log.bin multiple

# Create test samples
python process_binary_file.py sample
```

### Detection Testing

```bash
# Interactive testing with multiple formats
python test_kafka_detection.py interactive

# Automated detection tests
python test_kafka_detection.py

# Byte order debugging
python debug_byte_order.py interactive
python debug_byte_order.py "087f0012000000003039"
```

### Analyzing Network Captures

```python
# 1. Capture Kafka traffic
# tcpdump -i any -s 0 -w kafka.pcap port 9092

# 2. Extract TCP payload (use Wireshark/tshark)

# 3. Check if it's Kafka data first
captured_bytes = bytes.fromhex("000000080012000000000001")
detection = is_kafka_binary(captured_bytes)
if detection['is_kafka']:
    decoded = decode_kafka_message(captured_bytes)
    print(f"Captured message: {decoded}")
else:
    print(f"Not Kafka data: {detection['reasons']}")
```

### Building Custom Decoders

```python
class CustomMessageDecoder(KafkaMessageDecoder):
    def decode_produce_request(self):
        """Add support for Produce requests"""
        header = self.decode_request_header()
        # Add Produce-specific decoding logic
        return {'header': header, 'type': 'produce'}

# Use your custom decoder
decoder = KafkaProtocolDecoder(data)
custom_decoder = CustomMessageDecoder(decoder)
```

## Testing

Comprehensive test suite included:

```bash
# Run basic decoder tests
python test_decoder.py

# Run Kafka detection tests
python test_kafka_detection.py

# Interactive testing with multiple formats
python test_kafka_detection.py interactive
python what_decoder.py interactive

# Test topic message decoding
python kafka_message_decoder.py "b'\x08\x7fhello\x00'"

# Debug byte order issues
python debug_byte_order.py interactive

# Run example demonstrations
python example_usage.py

# Run with pytest for detailed output
pip install pytest
pytest test_decoder.py -v
```

## Development

The decoder is designed for extensibility:

1. **Add new primitive types** in `KafkaProtocolDecoder`
2. **Add new message types** in `KafkaMessageDecoder` 
3. **Update constants** for new API keys/error codes
4. **Add tests** for any new functionality

## References

- [Kafka Protocol Guide](https://kafka.apache.org/protocol) - Official specification
- [KIP-482: Tagged Fields](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields) - Tagged fields specification
- [kafka-python](https://github.com/dpkp/kafka-python) - Pure Python Kafka client (good reference implementation)

## Project Files

### Core Decoders
- **`kafka_protocol_decoder.py`** - Wire protocol decoder (client↔broker communication)
- **`kafka_message_decoder.py`** - Topic message decoder (keys/values from topics)

### Analysis Tools  
- **`what_decoder.py`** - Smart format detection and decoder recommendations
- **`process_binary_file.py`** - Command-line tool for processing binary files
- **`debug_byte_order.py`** - Byte order debugging and endianness fixes

### Testing & Examples
- **`test_kafka_detection.py`** - Comprehensive detection testing suite
- **`test_decoder.py`** - Core decoder test suite  
- **`example_usage.py`** - Usage examples and demonstrations

### Documentation
- **`README.md`** - This documentation
- **`LICENSE`** - GNU GPL v3 license

## Data Type Guide

| Data Source | Use This Decoder | Example Data |
|-------------|------------------|--------------|
| **Network captures** (tcpdump, Wireshark) | `kafka_protocol_decoder.py` | `00000008 0012 0000...` |
| **Topic dumps** (kafka-console-consumer) | `kafka_message_decoder.py` | `087f0012 68656c6c6f...` |  
| **Log segments** (.log files) | `kafka_message_decoder.py` | Large offsets + message data |
| **Unknown format** | `what_decoder.py` | Auto-detects and recommends |

## Author

**Garland Glessner** <gglessner@gmail.com>

## License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This implementation is for educational and research purposes. Refer to Apache Kafka's license for protocol specification usage.
