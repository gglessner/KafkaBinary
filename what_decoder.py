"""
Which Kafka Decoder Should I Use?

This utility helps determine which decoder to use for your Kafka binary data.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0
"""

import sys


def explain_kafka_data_types():
    """Explain the different types of Kafka binary data"""
    
    print("Kafka Binary Data Types")
    print("=" * 50)
    print()
    
    print("1. WIRE PROTOCOL DATA (Client ↔ Broker Communication)")
    print("   - Use: kafka_protocol_decoder.py")
    print("   - Contains: API requests/responses (Produce, Fetch, Metadata, etc.)")
    print("   - Structure: [Length][APIKey][APIVersion][CorrelationID][Payload]")
    print("   - Example: API_VERSIONS request, METADATA response")
    print("   - Captured from: Network traffic, broker logs")
    print("   - Starts with: Message length (usually small, < 1MB)")
    print()
    
    print("2. TOPIC MESSAGE DATA (Message Keys & Values)")
    print("   - Use: kafka_message_decoder.py")
    print("   - Contains: Actual messages stored in Kafka topics")
    print("   - Structure: [Offset][MessageSize][CRC][Magic][Key][Value]")
    print("   - Example: JSON data, strings, binary payloads sent by producers")
    print("   - Captured from: kafka-console-consumer, topic dumps, log segments")
    print("   - Starts with: Offset (usually large number)")
    print()
    
    print("3. LOG SEGMENT DATA (Physical Storage)")
    print("   - Use: kafka_message_decoder.py")
    print("   - Contains: How messages are stored on disk")
    print("   - Structure: Batch headers + compressed message sets")
    print("   - Example: .log files from Kafka data directory")
    print("   - Starts with: Base offset (large number)")
    print()


def parse_python_bytes_string(byte_string: str) -> bytes:
    """
    Parse Python byte string format like b'\x77hello\x00' into actual bytes
    (Same function as in kafka_message_decoder.py)
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


def analyze_data_type(data_input):
    """Analyze data to suggest which decoder to use"""
    
    # Determine input format and parse accordingly
    if data_input.startswith("b'") or data_input.startswith('b"') or '\\x' in data_input:
        # Python bytes string format
        try:
            data = parse_python_bytes_string(data_input)
            input_type = "Python bytes string"
        except Exception:
            print("Error: Invalid Python bytes string format")
            return
    else:
        # Hex string format
        try:
            data = bytes.fromhex(data_input.replace(' ', ''))
            input_type = "Hex string"
        except ValueError:
            print("Error: Invalid hex string")
            return
    
    print(f"Analyzing: {data_input[:50]}...")
    print(f"Input type: {input_type}")
    print(f"Length: {len(data)} bytes")
    print(f"Hex representation: {data[:16].hex()}{'...' if len(data) > 16 else ''}")
    print(f"Python bytes format: {repr(data[:32])}")
    print()
    
    if len(data) < 4:
        print("Data too short to analyze")
        return
    
    # Check first 4 bytes as different interpretations
    import struct
    
    first_4_be = struct.unpack('>I', data[:4])[0]
    first_4_le = struct.unpack('<I', data[:4])[0]
    
    print("First 4 bytes interpretations:")
    print(f"  Big-endian uint32: {first_4_be:,}")
    print(f"  Little-endian uint32: {first_4_le:,}")
    print()
    
    # Heuristics for data type
    suggestions = []
    
    # Wire protocol heuristics
    if 8 <= first_4_be <= 100_000:  # Reasonable message length
        if len(data) >= 8:
            api_key = struct.unpack('>H', data[4:6])[0]
            if 0 <= api_key <= 100:
                suggestions.append({
                    'type': 'Wire Protocol',
                    'decoder': 'kafka_protocol_decoder.py',
                    'confidence': 'High',
                    'reason': f'Small length ({first_4_be}) + valid API key ({api_key})'
                })
    
    # Topic message heuristics  
    if first_4_be > 1_000_000:  # Large offset
        suggestions.append({
            'type': 'Topic Message (offset)',
            'decoder': 'kafka_message_decoder.py', 
            'confidence': 'Medium',
            'reason': f'Large first value ({first_4_be:,}) suggests offset'
        })
    
    # Look for magic bytes
    for i in range(min(20, len(data))):
        if data[i] in [0, 1, 2]:
            suggestions.append({
                'type': 'Topic Message (magic byte)',
                'decoder': 'kafka_message_decoder.py',
                'confidence': 'Medium', 
                'reason': f'Magic byte {data[i]} at position {i}'
            })
            break
    
    # Raw content heuristics
    try:
        decoded_str = data.decode('utf-8')
        if decoded_str.isprintable():
            suggestions.append({
                'type': 'Raw Message Content',
                'decoder': 'kafka_message_decoder.py',
                'confidence': 'Low',
                'reason': 'Data contains readable text'
            })
    except:
        pass
    
    # Present suggestions
    if suggestions:
        print("RECOMMENDATIONS:")
        for i, suggestion in enumerate(suggestions, 1):
            print(f"{i}. {suggestion['type']} ({suggestion['confidence']} confidence)")
            print(f"   Use: {suggestion['decoder']}")
            print(f"   Reason: {suggestion['reason']}")
            print()
    else:
        print("UNABLE TO DETERMINE DATA TYPE")
        print("Try both decoders:")
        print("1. kafka_protocol_decoder.py (for wire protocol)")
        print("2. kafka_message_decoder.py (for topic messages)")
        print()
    
    # Show commands to try
    print("COMMANDS TO TRY:")
    if input_type == "Python bytes string":
        print(f"python kafka_message_decoder.py \"{data_input}\"")
        print(f"python kafka_protocol_decoder.py # Convert to hex first: {data.hex()}")
    else:
        print(f"python kafka_protocol_decoder.py # Add your data")
        print(f"python kafka_message_decoder.py {data_input}")
    print(f"python process_binary_file.py <your_file.bin>")


def interactive_help():
    """Interactive help mode"""
    
    print("Interactive Kafka Decoder Helper")
    print("=" * 40)
    
    questions = [
        {
            'question': "Where did you get this binary data?",
            'options': {
                '1': 'Network capture (tcpdump, Wireshark)',
                '2': 'Kafka topic dump (kafka-console-consumer)',
                '3': 'Kafka log files (.log files from data directory)', 
                '4': 'Other/Unknown'
            }
        },
        {
            'question': "What are you trying to decode?",
            'options': {
                '1': 'Client-broker communication',
                '2': 'Message content (keys/values from topics)',
                '3': 'Internal Kafka storage format',
                '4': 'Not sure'
            }
        }
    ]
    
    answers = []
    
    for q in questions:
        print(f"\n{q['question']}")
        for key, value in q['options'].items():
            print(f"  {key}. {value}")
        
        while True:
            answer = input("Choose (1-4): ").strip()
            if answer in q['options']:
                answers.append(answer)
                break
            print("Please enter 1, 2, 3, or 4")
    
    # Provide recommendation based on answers
    print("\nRECOMMENDATION:")
    
    source_answer, purpose_answer = answers
    
    if source_answer == '1' or purpose_answer == '1':
        print("Use: kafka_protocol_decoder.py")
        print("Reason: Network captures contain wire protocol data")
    elif source_answer in ['2', '3'] or purpose_answer in ['2', '3']:
        print("Use: kafka_message_decoder.py") 
        print("Reason: Topic dumps and log files contain message data")
    else:
        print("Try both decoders:")
        print("1. kafka_protocol_decoder.py (for protocol messages)")
        print("2. kafka_message_decoder.py (for topic content)")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "interactive":
            explain_kafka_data_types()
            print()
            interactive_help()
        elif sys.argv[1] == "explain":
            explain_kafka_data_types()
        else:
            # Analyze provided hex data
            analyze_data_type(sys.argv[1])
    else:
        print("Kafka Decoder Selection Tool")
        print("=" * 35)
        print()
        print("Usage:")
        print("  python what_decoder.py explain          # Explain data types")
        print("  python what_decoder.py interactive      # Interactive help")
        print("  python what_decoder.py <hex_data>       # Analyze hex data")
        print("  python what_decoder.py <python_bytes>   # Analyze Python bytes")
        print()
        print("Examples:")
        print("  python what_decoder.py 087f0012000000003039")
        print("  python what_decoder.py \"b'\\x08\\x7fhello\\x00'\"")
        print("  python what_decoder.py \"\\x08\\x7fhello world\\x00\"")
        print("  python what_decoder.py interactive")
        print()
        print("Quick Guide:")
        print("- Wire protocol (client-broker): use kafka_protocol_decoder.py")
        print("- Topic messages (keys/values): use kafka_message_decoder.py")
