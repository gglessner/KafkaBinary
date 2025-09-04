"""
Kafka Message Content Decoder

This decoder handles Kafka message keys and values (topic data), not the wire protocol.
The wire protocol decoder is for client-broker communication, while this handles the actual
message content stored in Kafka topics.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0
"""

import struct
import json
import io
from typing import Optional, Dict, Any, Union
from enum import IntEnum


class CompressionType(IntEnum):
    """Kafka compression types"""
    NONE = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3
    ZSTD = 4


class KafkaMessageContentDecoder:
    """Decoder for Kafka message content (keys and values from topics)"""
    
    def __init__(self, data: bytes):
        """Initialize with raw message data"""
        self.data = data
        self.buffer = io.BytesIO(data)
        self.position = 0
    
    def decode_message_set_v0_v1(self) -> Dict[str, Any]:
        """
        Decode legacy message format (v0/v1) - used in older Kafka versions
        Format: [Offset][MessageSize][CRC][Magic][Attributes][Key][Value]
        """
        if len(self.data) < 12:
            raise ValueError("Data too short for message set")
        
        # Read message metadata
        offset = struct.unpack('>Q', self.data[0:8])[0]
        message_size = struct.unpack('>I', self.data[8:12])[0]
        
        if len(self.data) < 12 + message_size:
            raise ValueError(f"Incomplete message: need {12 + message_size} bytes, got {len(self.data)}")
        
        # Parse message content
        message_data = self.data[12:12 + message_size]
        
        crc = struct.unpack('>I', message_data[0:4])[0]
        magic = struct.unpack('B', message_data[4:5])[0]
        attributes = struct.unpack('B', message_data[5:6])[0]
        
        # Extract compression type from attributes
        compression = attributes & 0x07
        
        pos = 6
        
        # Read key
        key_length = struct.unpack('>i', message_data[pos:pos+4])[0]
        pos += 4
        
        if key_length == -1:
            key = None
        else:
            key = message_data[pos:pos + key_length]
            pos += key_length
        
        # Read value
        value_length = struct.unpack('>i', message_data[pos:pos+4])[0]
        pos += 4
        
        if value_length == -1:
            value = None
        else:
            value = message_data[pos:pos + value_length]
        
        return {
            'format': 'MessageSet v0/v1',
            'offset': offset,
            'message_size': message_size,
            'crc': crc,
            'magic': magic,
            'compression_type': compression,
            'key': key,
            'value': value,
            'key_decoded': self.try_decode_content(key),
            'value_decoded': self.try_decode_content(value)
        }
    
    def decode_record_batch_v2(self) -> Dict[str, Any]:
        """
        Decode modern record batch format (v2+) - used in newer Kafka versions
        This is more complex but more efficient
        """
        if len(self.data) < 61:  # Minimum record batch header size
            raise ValueError("Data too short for record batch")
        
        # Record batch header
        base_offset = struct.unpack('>Q', self.data[0:8])[0]
        batch_length = struct.unpack('>I', self.data[8:12])[0]
        partition_leader_epoch = struct.unpack('>I', self.data[12:16])[0]
        magic = struct.unpack('B', self.data[16:17])[0]
        crc = struct.unpack('>I', self.data[17:21])[0]
        attributes = struct.unpack('>H', self.data[21:23])[0]
        last_offset_delta = struct.unpack('>I', self.data[23:27])[0]
        first_timestamp = struct.unpack('>Q', self.data[27:35])[0]
        max_timestamp = struct.unpack('>Q', self.data[35:43])[0]
        producer_id = struct.unpack('>Q', self.data[43:51])[0]
        producer_epoch = struct.unpack('>H', self.data[51:53])[0]
        base_sequence = struct.unpack('>I', self.data[53:57])[0]
        records_count = struct.unpack('>I', self.data[57:61])[0]
        
        compression_type = attributes & 0x07
        
        return {
            'format': 'RecordBatch v2+',
            'base_offset': base_offset,
            'batch_length': batch_length,
            'magic': magic,
            'compression_type': compression_type,
            'records_count': records_count,
            'first_timestamp': first_timestamp,
            'max_timestamp': max_timestamp,
            'producer_id': producer_id,
            'note': 'Individual record parsing requires handling variable-length encoding'
        }
    
    def try_decode_content(self, content: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """
        Try to decode message content as various formats
        """
        if content is None:
            return None
        
        result = {
            'raw_bytes': content,
            'length': len(content),
            'hex': content.hex(),
        }
        
        # Try UTF-8 string
        try:
            decoded_str = content.decode('utf-8')
            result['utf8'] = decoded_str
            result['is_printable'] = decoded_str.isprintable()
        except UnicodeDecodeError:
            result['utf8'] = None
            result['is_printable'] = False
        
        # Try JSON
        if result.get('utf8'):
            try:
                result['json'] = json.loads(result['utf8'])
                result['format_detected'] = 'JSON'
            except json.JSONDecodeError:
                pass
        
        # Try to detect if it's a number
        if len(content) in [1, 2, 4, 8]:
            try:
                if len(content) == 1:
                    result['int8'] = struct.unpack('b', content)[0]
                    result['uint8'] = struct.unpack('B', content)[0]
                elif len(content) == 2:
                    result['int16_be'] = struct.unpack('>h', content)[0]
                    result['int16_le'] = struct.unpack('<h', content)[0]
                elif len(content) == 4:
                    result['int32_be'] = struct.unpack('>i', content)[0]
                    result['int32_le'] = struct.unpack('<i', content)[0]
                    result['float32_be'] = struct.unpack('>f', content)[0]
                elif len(content) == 8:
                    result['int64_be'] = struct.unpack('>q', content)[0]
                    result['int64_le'] = struct.unpack('<q', content)[0]
                    result['float64_be'] = struct.unpack('>d', content)[0]
            except struct.error:
                pass
        
        return result
    
    def auto_detect_format(self) -> str:
        """
        Auto-detect the Kafka message format
        """
        if len(self.data) < 12:
            return "unknown"
        
        # Check for magic byte which indicates format
        # Magic byte locations differ between formats
        possible_magic_positions = [16, 21]  # v2+ format
        
        for pos in possible_magic_positions:
            if pos < len(self.data):
                magic = self.data[pos]
                if magic == 2:
                    return "record_batch_v2"
        
        # Check for v0/v1 format (magic at position 16 in message, not batch)
        if len(self.data) >= 17:
            magic = self.data[16]
            if magic in [0, 1]:
                return "message_set_v0_v1"
        
        return "unknown"


def decode_kafka_topic_data(data: bytes) -> Dict[str, Any]:
    """
    Main entry point to decode Kafka topic data (message keys/values)
    
    Args:
        data: Raw binary data from Kafka topic dump
        
    Returns:
        Decoded message information
    """
    decoder = KafkaMessageContentDecoder(data)
    
    try:
        format_type = decoder.auto_detect_format()
        
        if format_type == "message_set_v0_v1":
            return decoder.decode_message_set_v0_v1()
        elif format_type == "record_batch_v2":
            return decoder.decode_record_batch_v2()
        else:
            # If we can't detect format, treat as raw key/value content
            return {
                'format': 'Raw Content',
                'content_analysis': decoder.try_decode_content(data),
                'note': 'Could not detect Kafka message format - treating as raw content'
            }
    
    except Exception as e:
        # Fallback: analyze as raw content
        return {
            'format': 'Raw Content (fallback)',
            'error': str(e),
            'content_analysis': decoder.try_decode_content(data),
        }


def parse_python_bytes_string(byte_string: str) -> bytes:
    """
    Parse Python byte string format like b'\x77hello\x00' into actual bytes
    
    Args:
        byte_string: String in format like "b'\x77hello\x00'" or "\\x77hello\\x00"
        
    Returns:
        Actual bytes object
    """
    # Remove b' prefix and ' suffix if present
    if byte_string.startswith("b'") and byte_string.endswith("'"):
        byte_string = byte_string[2:-1]
    elif byte_string.startswith('b"') and byte_string.endswith('"'):
        byte_string = byte_string[2:-1]
    
    # Handle escape sequences
    result = b''
    i = 0
    while i < len(byte_string):
        if byte_string[i] == '\\' and i + 1 < len(byte_string):
            next_char = byte_string[i + 1]
            if next_char == 'x' and i + 3 < len(byte_string):
                # Handle \x## hex escape
                try:
                    hex_value = int(byte_string[i+2:i+4], 16)
                    result += bytes([hex_value])
                    i += 4
                except ValueError:
                    # Invalid hex, treat as literal
                    result += byte_string[i].encode('latin-1')
                    i += 1
            elif next_char == 'n':
                result += b'\n'
                i += 2
            elif next_char == 't':
                result += b'\t'
                i += 2
            elif next_char == 'r':
                result += b'\r'
                i += 2
            elif next_char == '\\':
                result += b'\\'
                i += 2
            elif next_char == "'":
                result += b"'"
                i += 2
            elif next_char == '"':
                result += b'"'
                i += 2
            elif next_char == '0':
                result += b'\x00'
                i += 2
            else:
                # Unknown escape, treat as literal
                result += byte_string[i].encode('latin-1')
                i += 1
        else:
            # Regular character
            result += byte_string[i].encode('latin-1')
            i += 1
    
    return result


def analyze_topic_dump_format(data: bytes) -> None:
    """
    Analyze and print information about topic data format
    """
    print(f"Data length: {len(data)} bytes")
    print(f"First 32 bytes (hex): {data[:32].hex()}")
    print(f"Python bytes format: {repr(data[:32])}")
    
    if len(data) >= 20:
        print("\nPossible interpretations:")
        
        # Check different positions for magic bytes
        for i in range(min(20, len(data))):
            byte_val = data[i]
            if byte_val in [0, 1, 2]:
                print(f"  Position {i}: Magic byte {byte_val} (Kafka format v{byte_val})")
        
        # Look for common patterns
        print(f"\nFirst few bytes as integers:")
        print(f"  Big-endian int32: {struct.unpack('>I', data[:4])[0]}")
        print(f"  Little-endian int32: {struct.unpack('<I', data[:4])[0]}")
        
        if len(data) >= 8:
            print(f"  Big-endian int64: {struct.unpack('>Q', data[:8])[0]}")
    
    # Try to decode
    print(f"\nDecoding attempt:")
    result = decode_kafka_topic_data(data)
    
    print(f"Detected format: {result.get('format', 'Unknown')}")
    
    if 'key' in result:
        print(f"Key: {result['key']}")
        if result.get('key_decoded'):
            key_info = result['key_decoded']
            if key_info.get('utf8'):
                print(f"Key (UTF-8): {key_info['utf8']}")
    
    if 'value' in result:
        print(f"Value length: {len(result['value']) if result['value'] else 0} bytes")
        if result.get('value_decoded'):
            value_info = result['value_decoded']
            if value_info.get('utf8'):
                print(f"Value (UTF-8): {value_info['utf8'][:200]}{'...' if len(value_info['utf8']) > 200 else ''}")
            if value_info.get('json'):
                print(f"Value (JSON): {value_info['json']}")


if __name__ == "__main__":
    import sys
    
    print("Kafka Message Content Decoder")
    print("=" * 40)
    print("This decoder handles Kafka message keys and values from topics,")
    print("NOT the wire protocol communication between clients and brokers.")
    print()
    
    if len(sys.argv) > 1:
        if sys.argv[1].endswith('.bin'):
            # Treat as file
            try:
                with open(sys.argv[1], 'rb') as f:
                    data = f.read()
                print(f"Loading file: {sys.argv[1]}")
                analyze_topic_dump_format(data)
            except Exception as e:
                print(f"Error reading file: {e}")
        elif sys.argv[1].startswith("b'") or sys.argv[1].startswith('b"') or '\\x' in sys.argv[1]:
            # Treat as Python bytes string format
            try:
                data = parse_python_bytes_string(sys.argv[1])
                print(f"Analyzing Python bytes string: {sys.argv[1][:50]}...")
                analyze_topic_dump_format(data)
            except Exception as e:
                print(f"Error parsing Python bytes string: {e}")
        else:
            # Treat as hex string
            try:
                hex_str = sys.argv[1].replace(' ', '')
                data = bytes.fromhex(hex_str)
                print(f"Analyzing hex data: {hex_str[:32]}...")
                analyze_topic_dump_format(data)
            except Exception as e:
                print(f"Error parsing hex: {e}")
    else:
        print("Usage:")
        print("  python kafka_message_decoder.py <file.bin>")
        print("  python kafka_message_decoder.py <hex_string>")
        print("  python kafka_message_decoder.py <python_bytes_string>")
        print()
        print("Examples:")
        print("  python kafka_message_decoder.py topic_dump.bin")
        print("  python kafka_message_decoder.py 087f001200000000")
        print("  python kafka_message_decoder.py \"b'\\x77hello\\x00'\"")
        print("  python kafka_message_decoder.py \"\\x08\\x7fhello world\\x00\"")
        print()
        print("Note: If you're trying to decode wire protocol messages")
        print("(client-broker communication), use kafka_protocol_decoder.py instead.")
