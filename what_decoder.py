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


def analyze_data_type(data_hex):
    """Analyze data to suggest which decoder to use"""
    
    try:
        data = bytes.fromhex(data_hex.replace(' ', '').replace('\\x', ''))
    except ValueError:
        print("Error: Invalid hex string")
        return
    
    print(f"Analyzing: {data_hex[:32]}...")
    print(f"Length: {len(data)} bytes")
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
    print(f"python kafka_protocol_decoder.py # Add your data")
    print(f"python kafka_message_decoder.py {data_hex}")
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
        print("  python what_decoder.py explain      # Explain data types")
        print("  python what_decoder.py interactive  # Interactive help")
        print("  python what_decoder.py <hex_data>   # Analyze specific data")
        print()
        print("Examples:")
        print("  python what_decoder.py 087f0012000000003039")
        print("  python what_decoder.py interactive")
        print()
        print("Quick Guide:")
        print("- Wire protocol (client-broker): use kafka_protocol_decoder.py")
        print("- Topic messages (keys/values): use kafka_message_decoder.py")
