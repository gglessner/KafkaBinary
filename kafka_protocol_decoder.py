"""
Kafka Binary Protocol Decoder

A low-level Python decoder for Kafka's binary wire protocol based on the official
specification at https://kafka.apache.org/protocol

This implementation handles the core protocol primitive types and message structures
as defined in the Kafka protocol documentation.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""

import struct
import io
from typing import Dict, List, Tuple, Optional, Any, Union
from enum import IntEnum


class KafkaProtocolError(Exception):
    """Base exception for Kafka protocol parsing errors"""
    pass


class ErrorCode(IntEnum):
    """Kafka error codes as defined in the protocol specification"""
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNKNOWN_TOPIC_OR_PARTITION = 3
    INVALID_FETCH_SIZE = 4
    LEADER_NOT_AVAILABLE = 5
    NOT_LEADER_FOR_PARTITION = 6
    REQUEST_TIMED_OUT = 7
    BROKER_NOT_AVAILABLE = 8
    REPLICA_NOT_AVAILABLE = 9
    MESSAGE_TOO_LARGE = 10
    STALE_CONTROLLER_EPOCH = 11
    OFFSET_METADATA_TOO_LARGE = 12
    NETWORK_EXCEPTION = 13
    COORDINATOR_LOAD_IN_PROGRESS = 14
    COORDINATOR_NOT_AVAILABLE = 15
    NOT_COORDINATOR = 16
    INVALID_TOPIC_EXCEPTION = 17
    RECORD_LIST_TOO_LARGE = 18
    NOT_ENOUGH_REPLICAS = 19
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
    INVALID_REQUIRED_ACKS = 21
    ILLEGAL_GENERATION = 22
    INCONSISTENT_GROUP_PROTOCOL = 23
    INVALID_GROUP_ID = 24
    UNKNOWN_MEMBER_ID = 25
    INVALID_SESSION_TIMEOUT = 26
    REBALANCE_IN_PROGRESS = 27
    INVALID_COMMIT_OFFSET_SIZE = 28
    TOPIC_AUTHORIZATION_FAILED = 29
    GROUP_AUTHORIZATION_FAILED = 30
    CLUSTER_AUTHORIZATION_FAILED = 31


class ApiKey(IntEnum):
    """Kafka API keys as defined in the protocol specification"""
    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    LEADER_AND_ISR = 4
    STOP_REPLICA = 5
    UPDATE_METADATA = 6
    CONTROLLED_SHUTDOWN = 7
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    FIND_COORDINATOR = 10
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14
    DESCRIBE_GROUPS = 15
    LIST_GROUPS = 16
    SASL_HANDSHAKE = 17
    API_VERSIONS = 18
    CREATE_TOPICS = 19
    DELETE_TOPICS = 20
    DELETE_RECORDS = 21
    INIT_PRODUCER_ID = 22
    OFFSET_FOR_LEADER_EPOCH = 23
    ADD_PARTITIONS_TO_TXN = 24
    ADD_OFFSETS_TO_TXN = 25
    END_TXN = 26
    WRITE_TXN_MARKERS = 27
    TXN_OFFSET_COMMIT = 28
    DESCRIBE_ACLS = 29
    CREATE_ACLS = 30
    DELETE_ACLS = 31
    DESCRIBE_CONFIGS = 32
    ALTER_CONFIGS = 33


class KafkaProtocolDecoder:
    """Low-level decoder for Kafka binary protocol messages"""
    
    def __init__(self, data: bytes):
        """Initialize decoder with binary data"""
        self.data = data
        self.buffer = io.BytesIO(data)
        self.position = 0
    
    def reset(self):
        """Reset decoder position to beginning"""
        self.buffer.seek(0)
        self.position = 0
    
    def remaining_bytes(self) -> int:
        """Get number of remaining bytes in buffer"""
        return len(self.data) - self.position
    
    def read_int8(self) -> int:
        """Read signed 8-bit integer (1 byte)"""
        if self.remaining_bytes() < 1:
            raise KafkaProtocolError("Not enough bytes to read INT8")
        value = struct.unpack('>b', self.buffer.read(1))[0]
        self.position += 1
        return value
    
    def read_int16(self) -> int:
        """Read signed 16-bit integer (2 bytes) in network byte order"""
        if self.remaining_bytes() < 2:
            raise KafkaProtocolError("Not enough bytes to read INT16")
        value = struct.unpack('>h', self.buffer.read(2))[0]
        self.position += 2
        return value
    
    def read_int32(self) -> int:
        """Read signed 32-bit integer (4 bytes) in network byte order"""
        if self.remaining_bytes() < 4:
            raise KafkaProtocolError("Not enough bytes to read INT32")
        value = struct.unpack('>i', self.buffer.read(4))[0]
        self.position += 4
        return value
    
    def read_int64(self) -> int:
        """Read signed 64-bit integer (8 bytes) in network byte order"""
        if self.remaining_bytes() < 8:
            raise KafkaProtocolError("Not enough bytes to read INT64")
        value = struct.unpack('>q', self.buffer.read(8))[0]
        self.position += 8
        return value
    
    def read_uint32(self) -> int:
        """Read unsigned 32-bit integer (4 bytes) in network byte order"""
        if self.remaining_bytes() < 4:
            raise KafkaProtocolError("Not enough bytes to read UINT32")
        value = struct.unpack('>I', self.buffer.read(4))[0]
        self.position += 4
        return value
    
    def read_varint(self) -> int:
        """Read variable-length integer (used in newer protocol versions)"""
        result = 0
        shift = 0
        while True:
            if self.remaining_bytes() < 1:
                raise KafkaProtocolError("Not enough bytes to read VARINT")
            byte = struct.unpack('>B', self.buffer.read(1))[0]
            self.position += 1
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
            if shift >= 32:
                raise KafkaProtocolError("VARINT too long")
        return result
    
    def read_string(self) -> Optional[str]:
        """Read string prefixed with 16-bit length. Returns None for null strings."""
        length = self.read_int16()
        if length == -1:
            return None
        if length < 0:
            raise KafkaProtocolError(f"Invalid string length: {length}")
        if self.remaining_bytes() < length:
            raise KafkaProtocolError(f"Not enough bytes to read string of length {length}")
        
        data = self.buffer.read(length)
        self.position += length
        return data.decode('utf-8')
    
    def read_compact_string(self) -> Optional[str]:
        """Read compact string prefixed with varint length (newer protocol versions)"""
        length = self.read_varint() - 1  # Compact strings are length + 1
        if length == -1:
            return None
        if length < 0:
            raise KafkaProtocolError(f"Invalid compact string length: {length}")
        if self.remaining_bytes() < length:
            raise KafkaProtocolError(f"Not enough bytes to read compact string of length {length}")
        
        if length == 0:
            return ""
        
        data = self.buffer.read(length)
        self.position += length
        return data.decode('utf-8')
    
    def read_bytes(self) -> Optional[bytes]:
        """Read byte array prefixed with 32-bit length. Returns None for null arrays."""
        length = self.read_int32()
        if length == -1:
            return None
        if length < 0:
            raise KafkaProtocolError(f"Invalid bytes length: {length}")
        if self.remaining_bytes() < length:
            raise KafkaProtocolError(f"Not enough bytes to read {length} bytes")
        
        data = self.buffer.read(length)
        self.position += length
        return data
    
    def read_compact_bytes(self) -> Optional[bytes]:
        """Read compact byte array prefixed with varint length (newer protocol versions)"""
        length = self.read_varint() - 1  # Compact bytes are length + 1
        if length == -1:
            return None
        if length < 0:
            raise KafkaProtocolError(f"Invalid compact bytes length: {length}")
        if self.remaining_bytes() < length:
            raise KafkaProtocolError(f"Not enough bytes to read {length} compact bytes")
        
        if length == 0:
            return b""
        
        data = self.buffer.read(length)
        self.position += length
        return data
    
    def read_array(self, element_reader) -> List[Any]:
        """Read array prefixed with 32-bit length"""
        length = self.read_int32()
        if length == -1:
            return []
        if length < 0:
            raise KafkaProtocolError(f"Invalid array length: {length}")
        
        result = []
        for _ in range(length):
            result.append(element_reader())
        return result
    
    def read_compact_array(self, element_reader) -> List[Any]:
        """Read compact array prefixed with varint length (newer protocol versions)"""
        length = self.read_varint() - 1  # Compact arrays are length + 1
        if length == -1:
            return []
        if length < 0:
            raise KafkaProtocolError(f"Invalid compact array length: {length}")
        
        result = []
        for _ in range(length):
            result.append(element_reader())
        return result
    
    def read_uuid(self) -> str:
        """Read UUID as 16 bytes"""
        if self.remaining_bytes() < 16:
            raise KafkaProtocolError("Not enough bytes to read UUID")
        
        data = self.buffer.read(16)
        self.position += 16
        
        # Convert to standard UUID format
        uuid_hex = data.hex()
        return f"{uuid_hex[:8]}-{uuid_hex[8:12]}-{uuid_hex[12:16]}-{uuid_hex[16:20]}-{uuid_hex[20:]}"
    
    def read_tagged_fields(self) -> Dict[int, bytes]:
        """Read tagged fields (KIP-482) used in newer protocol versions"""
        num_fields = self.read_varint()
        fields = {}
        
        for _ in range(num_fields):
            tag = self.read_varint()
            size = self.read_varint()
            if self.remaining_bytes() < size:
                raise KafkaProtocolError(f"Not enough bytes to read tagged field of size {size}")
            data = self.buffer.read(size)
            self.position += size
            fields[tag] = data
        
        return fields


class KafkaMessageDecoder:
    """High-level decoder for Kafka messages and protocol structures"""
    
    def __init__(self, decoder: KafkaProtocolDecoder):
        self.decoder = decoder
    
    def decode_request_header(self, api_version: int = 0) -> Dict[str, Any]:
        """Decode Kafka request header"""
        if api_version >= 1:
            # Request Header v1+ includes client_id
            return {
                'api_key': self.decoder.read_int16(),
                'api_version': self.decoder.read_int16(),
                'correlation_id': self.decoder.read_int32(),
                'client_id': self.decoder.read_string(),
            }
        else:
            # Request Header v0
            return {
                'api_key': self.decoder.read_int16(),
                'api_version': self.decoder.read_int16(),
                'correlation_id': self.decoder.read_int32(),
            }
    
    def decode_response_header(self, api_version: int = 0) -> Dict[str, Any]:
        """Decode Kafka response header"""
        return {
            'correlation_id': self.decoder.read_int32(),
        }
    
    def decode_record_batch_header(self) -> Dict[str, Any]:
        """Decode record batch header"""
        return {
            'base_offset': self.decoder.read_int64(),
            'batch_length': self.decoder.read_int32(),
            'partition_leader_epoch': self.decoder.read_int32(),
            'magic': self.decoder.read_int8(),
            'crc': self.decoder.read_uint32(),
            'attributes': self.decoder.read_int16(),
            'last_offset_delta': self.decoder.read_int32(),
            'first_timestamp': self.decoder.read_int64(),
            'max_timestamp': self.decoder.read_int64(),
            'producer_id': self.decoder.read_int64(),
            'producer_epoch': self.decoder.read_int16(),
            'base_sequence': self.decoder.read_int32(),
            'records_count': self.decoder.read_int32(),
        }
    
    def decode_api_versions_request(self) -> Dict[str, Any]:
        """Decode ApiVersions request (typically the first request sent)"""
        header = self.decode_request_header()
        
        # ApiVersionsRequest has no body in v0
        if header['api_version'] >= 1:
            client_software_name = self.decoder.read_string()
            client_software_version = self.decoder.read_string()
            return {
                'header': header,
                'client_software_name': client_software_name,
                'client_software_version': client_software_version,
            }
        
        return {'header': header}
    
    def decode_metadata_request(self) -> Dict[str, Any]:
        """Decode Metadata request"""
        header = self.decode_request_header()
        
        topics = self.decoder.read_array(lambda: self.decoder.read_string())
        
        result = {
            'header': header,
            'topics': topics,
        }
        
        # Additional fields in newer versions
        if header['api_version'] >= 4:
            result['allow_auto_topic_creation'] = self.decoder.read_int8() != 0
        
        return result


def is_kafka_binary(data: bytes) -> Dict[str, Any]:
    """
    Detect if binary data is likely Kafka protocol data
    
    Args:
        data: Raw binary data to analyze
        
    Returns:
        Dictionary containing:
        - is_kafka: Boolean indicating if data appears to be Kafka
        - confidence: Float 0-1 indicating confidence level
        - reasons: List of reasons for the determination
        - detected_api_key: API key if detected (or None)
        - detected_version: API version if detected (or None)
    """
    reasons = []
    confidence = 0.0
    detected_api_key = None
    detected_version = None
    
    if len(data) < 8:
        return {
            'is_kafka': False,
            'confidence': 0.0,
            'reasons': ['Data too short (minimum 8 bytes required)'],
            'detected_api_key': None,
            'detected_version': None
        }
    
    try:
        # Check if it starts with a reasonable message length
        import struct
        message_length = struct.unpack('>I', data[:4])[0]
        
        # Kafka message length should be reasonable (not too big, not negative)
        if message_length <= 0:
            reasons.append('Invalid message length (negative or zero)')
            return {
                'is_kafka': False,
                'confidence': 0.0,
                'reasons': reasons,
                'detected_api_key': None,
                'detected_version': None
            }
        
        if message_length > 100 * 1024 * 1024:  # 100MB seems unreasonable
            reasons.append(f'Message length too large ({message_length} bytes)')
            confidence -= 0.3
        elif message_length > len(data) - 4:
            reasons.append(f'Message length ({message_length}) exceeds available data ({len(data) - 4})')
            confidence -= 0.2
        else:
            reasons.append(f'Reasonable message length: {message_length} bytes')
            confidence += 0.3
        
        # Check API key (should be a known value)
        if len(data) >= 6:
            api_key = struct.unpack('>H', data[4:6])[0]
            detected_api_key = api_key
            
            # Known Kafka API keys (from ApiKey enum)
            known_api_keys = set(range(0, 93))  # Current range of valid API keys
            
            if api_key in known_api_keys:
                reasons.append(f'Valid API key: {api_key}')
                confidence += 0.4
                
                # Give extra confidence for common API keys
                common_apis = {0, 1, 3, 18}  # PRODUCE, FETCH, METADATA, API_VERSIONS
                if api_key in common_apis:
                    reasons.append(f'Common API key: {api_key}')
                    confidence += 0.1
            else:
                reasons.append(f'Unknown API key: {api_key}')
                confidence -= 0.3
        
        # Check API version (should be reasonable)
        if len(data) >= 8:
            api_version = struct.unpack('>H', data[6:8])[0]
            detected_version = api_version
            
            if 0 <= api_version <= 20:  # Most API versions are in this range
                reasons.append(f'Reasonable API version: {api_version}')
                confidence += 0.2
            else:
                reasons.append(f'Unusual API version: {api_version}')
                confidence -= 0.2
        
        # Check correlation ID (should be reasonable)
        if len(data) >= 12:
            correlation_id = struct.unpack('>I', data[8:12])[0]
            
            # Correlation IDs are typically positive and not too large
            if 0 <= correlation_id <= 0x7FFFFFFF:
                reasons.append(f'Reasonable correlation ID: {correlation_id}')
                confidence += 0.1
            else:
                reasons.append(f'Unusual correlation ID: {correlation_id}')
                confidence -= 0.1
        
        # Check for valid UTF-8 strings in likely positions
        # This is heuristic - look for client_id in newer protocol versions
        if len(data) >= 16 and detected_api_key is not None and detected_version is not None:
            try:
                # Try to extract potential client_id (if API version >= 1)
                if detected_version >= 1:
                    client_id_length = struct.unpack('>H', data[12:14])[0]
                    if 0 <= client_id_length <= 256:  # Reasonable client ID length
                        if len(data) >= 14 + client_id_length:
                            client_id_bytes = data[14:14 + client_id_length]
                            client_id = client_id_bytes.decode('utf-8')
                            if client_id.isprintable():
                                reasons.append(f'Valid UTF-8 client ID: "{client_id}"')
                                confidence += 0.2
            except (UnicodeDecodeError, struct.error):
                pass  # Not necessarily a problem
        
        # Additional heuristic: check if data can be partially decoded
        try:
            decoder = KafkaProtocolDecoder(data[4:4 + min(message_length, len(data) - 4)])
            api_key_check = decoder.read_int16()
            api_version_check = decoder.read_int16()
            correlation_id_check = decoder.read_int32()
            
            if (api_key_check == detected_api_key and 
                api_version_check == detected_version):
                reasons.append('Successfully parsed header structure')
                confidence += 0.1
        except:
            reasons.append('Failed to parse as Kafka header')
            confidence -= 0.2
        
        # Normalize confidence to 0-1 range
        confidence = max(0.0, min(1.0, confidence))
        
        # Determine if it's likely Kafka based on confidence
        is_kafka = confidence >= 0.5
        
        return {
            'is_kafka': is_kafka,
            'confidence': confidence,
            'reasons': reasons,
            'detected_api_key': detected_api_key,
            'detected_version': detected_version
        }
        
    except Exception as e:
        return {
            'is_kafka': False,
            'confidence': 0.0,
            'reasons': [f'Error during analysis: {e}'],
            'detected_api_key': None,
            'detected_version': None
        }


def decode_kafka_message(data: bytes) -> Dict[str, Any]:
    """
    Main entry point to decode a complete Kafka protocol message
    
    Args:
        data: Raw binary data from Kafka wire protocol
        
    Returns:
        Decoded message structure
    """
    if len(data) < 4:
        raise KafkaProtocolError("Message too short - missing length prefix")
    
    # Read message length (first 4 bytes)
    message_length = struct.unpack('>I', data[:4])[0]
    
    if len(data) < message_length + 4:
        raise KafkaProtocolError(f"Incomplete message: expected {message_length + 4} bytes, got {len(data)}")
    
    # Create decoder for the message payload (excluding length prefix)
    decoder = KafkaProtocolDecoder(data[4:4 + message_length])
    message_decoder = KafkaMessageDecoder(decoder)
    
    # Peek at API key to determine message type
    api_key = struct.unpack('>H', data[4:6])[0]
    
    try:
        if api_key == ApiKey.API_VERSIONS:
            return {
                'message_type': 'ApiVersionsRequest',
                'api_key': api_key,
                'data': message_decoder.decode_api_versions_request()
            }
        elif api_key == ApiKey.METADATA:
            return {
                'message_type': 'MetadataRequest',
                'api_key': api_key,
                'data': message_decoder.decode_metadata_request()
            }
        else:
            # For unknown API keys, just decode the header
            header = message_decoder.decode_request_header()
            return {
                'message_type': 'UnknownRequest',
                'api_key': api_key,
                'data': {'header': header}
            }
    except Exception as e:
        raise KafkaProtocolError(f"Failed to decode message: {e}")


if __name__ == "__main__":
    # Example usage
    print("Kafka Protocol Decoder")
    print("======================")
    print()
    print("This module provides low-level decoding of Kafka's binary wire protocol.")
    print("Usage:")
    print("  decoder = KafkaProtocolDecoder(binary_data)")
    print("  message = decode_kafka_message(complete_message_bytes)")
    print()
    print("Supported primitive types:")
    print("  - INT8, INT16, INT32, INT64")
    print("  - UINT32, VARINT")
    print("  - STRING, COMPACT_STRING")
    print("  - BYTES, COMPACT_BYTES")
    print("  - ARRAY, COMPACT_ARRAY")
    print("  - UUID, TAGGED_FIELDS")
