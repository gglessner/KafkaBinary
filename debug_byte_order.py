"""
Debug Byte Order Issues in Kafka Binary Data

This script helps diagnose and fix byte order problems in binary Kafka data.

Author: Garland Glessner <gglessner@gmail.com>
License: GNU General Public License v3.0
"""

import struct
from kafka_protocol_decoder import is_kafka_binary, decode_kafka_message, KafkaProtocolError


def analyze_byte_order(hex_string):
    """
    Analyze binary data with different byte order interpretations
    
    Args:
        hex_string: Hex string like "087f..." or raw binary data
    """
    
    print(f"Analyzing data starting with: {hex_string[:8]}...")
    print("=" * 60)
    
    # Convert hex string to bytes if needed
    if isinstance(hex_string, str):
        try:
            data = bytes.fromhex(hex_string.replace(' ', ''))
        except ValueError:
            print("Error: Invalid hex string")
            return
    else:
        data = hex_string
    
    print(f"Raw data length: {len(data)} bytes")
    print(f"First 16 bytes (hex): {data[:16].hex()}")
    print()
    
    # Test different interpretations
    interpretations = [
        ("Original (Big-Endian)", data, '>'),
        ("Reversed Bytes", data[::-1], '>'),
        ("Little-Endian Length", swap_length_endian(data), '>'),
        ("Full Little-Endian", data, '<'),
        ("Word-Swapped", word_swap(data), '>'),
    ]
    
    for name, test_data, endian in interpretations:
        print(f"--- {name} ---")
        analyze_interpretation(test_data, endian)
        print()


def swap_length_endian(data):
    """Swap just the length prefix from big to little endian"""
    if len(data) < 4:
        return data
    
    # Read first 4 bytes as little-endian, convert to big-endian
    length_le = struct.unpack('<I', data[:4])[0]
    length_be = struct.pack('>I', length_le)
    
    return length_be + data[4:]


def word_swap(data):
    """Swap 16-bit words (sometimes files have word-swapped data)"""
    if len(data) % 2 != 0:
        data = data + b'\x00'  # Pad if odd length
    
    swapped = b''
    for i in range(0, len(data), 2):
        swapped += data[i+1:i+2] + data[i:i+1]
    
    return swapped


def analyze_interpretation(data, endian_format):
    """Analyze a specific interpretation of the data"""
    
    if len(data) < 8:
        print(f"  Too short: {len(data)} bytes")
        return
    
    try:
        # Try to parse as Kafka message length + header
        if endian_format == '>':
            length = struct.unpack('>I', data[:4])[0]
            api_key = struct.unpack('>H', data[4:6])[0]
            api_version = struct.unpack('>H', data[6:8])[0]
        else:
            length = struct.unpack('<I', data[:4])[0]
            api_key = struct.unpack('<H', data[4:6])[0]
            api_version = struct.unpack('<H', data[6:8])[0]
        
        print(f"  Length: {length}")
        print(f"  API Key: {api_key}")
        print(f"  API Version: {api_version}")
        
        # Check if this looks reasonable
        if 0 <= length <= 100*1024*1024 and 0 <= api_key <= 100 and 0 <= api_version <= 20:
            print(f"  Status: REASONABLE")
            
            # Try Kafka detection
            detection = is_kafka_binary(data)
            print(f"  Kafka Detection: {detection['is_kafka']} (confidence: {detection['confidence']:.2f})")
            
            if detection['is_kafka']:
                try:
                    decoded = decode_kafka_message(data)
                    print(f"  SUCCESS: {decoded['message_type']}")
                except Exception as e:
                    print(f"  Decode failed: {e}")
        else:
            print(f"  Status: UNREASONABLE")
            
    except Exception as e:
        print(f"  Parse error: {e}")


def fix_byte_order_interactive():
    """Interactive byte order fixing"""
    
    print("Interactive Byte Order Fixer")
    print("=" * 40)
    print("Enter your hex data (without spaces or prefixes):")
    print("Example: 087f0012000000003039")
    
    while True:
        try:
            user_input = input("\nHex data (or 'quit'): ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                break
            
            if not user_input:
                continue
            
            # Clean input
            user_input = user_input.replace(' ', '').replace('\\x', '').replace('0x', '')
            
            analyze_byte_order(user_input)
            
            # Ask if they want to try a fix
            print("\nWould you like to try a specific fix? (y/n)")
            if input().lower().startswith('y'):
                suggest_fixes(user_input)
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


def suggest_fixes(hex_string):
    """Suggest specific fixes for the data"""
    
    print("\nTrying specific fixes:")
    print("-" * 30)
    
    data = bytes.fromhex(hex_string)
    
    # Fix 1: Swap length endianness
    if len(data) >= 4:
        fixed1 = swap_length_endian(data)
        print("Fix 1 - Swap length endianness:")
        print(f"  Original: {data[:8].hex()}")
        print(f"  Fixed:    {fixed1[:8].hex()}")
        
        detection = is_kafka_binary(fixed1)
        if detection['is_kafka']:
            print(f"  SUCCESS! Confidence: {detection['confidence']:.2f}")
            try:
                decoded = decode_kafka_message(fixed1)
                print(f"  Message: {decoded['message_type']}")
            except:
                pass
        else:
            print(f"  No improvement")
        print()
    
    # Fix 2: Reverse entire data
    fixed2 = data[::-1]
    print("Fix 2 - Reverse entire data:")
    print(f"  Original: {data[:8].hex()}")
    print(f"  Fixed:    {fixed2[:8].hex()}")
    
    detection = is_kafka_binary(fixed2)
    if detection['is_kafka']:
        print(f"  SUCCESS! Confidence: {detection['confidence']:.2f}")
        try:
            decoded = decode_kafka_message(fixed2)
            print(f"  Message: {decoded['message_type']}")
        except:
            pass
    else:
        print(f"  No improvement")
    print()
    
    # Fix 3: Word swap
    fixed3 = word_swap(data)
    print("Fix 3 - Word swap (16-bit):")
    print(f"  Original: {data[:8].hex()}")
    print(f"  Fixed:    {fixed3[:8].hex()}")
    
    detection = is_kafka_binary(fixed3)
    if detection['is_kafka']:
        print(f"  SUCCESS! Confidence: {detection['confidence']:.2f}")
        try:
            decoded = decode_kafka_message(fixed3)
            print(f"  Message: {decoded['message_type']}")
        except:
            pass
    else:
        print(f"  No improvement")


def create_byte_order_test_file():
    """Create test files with different byte orders for testing"""
    
    # Create a normal Kafka message
    normal_data = bytes.fromhex("000000080012000000003039")
    
    # Create variations
    little_endian = swap_length_endian(normal_data)
    reversed_data = normal_data[::-1]
    word_swapped = word_swap(normal_data)
    
    # Write test files
    with open("normal_kafka.bin", "wb") as f:
        f.write(normal_data)
    
    with open("little_endian_length.bin", "wb") as f:
        f.write(little_endian)
    
    with open("reversed_kafka.bin", "wb") as f:
        f.write(reversed_data)
    
    with open("word_swapped_kafka.bin", "wb") as f:
        f.write(word_swapped)
    
    print("Created test files:")
    print("- normal_kafka.bin (standard)")
    print("- little_endian_length.bin (LE length)")
    print("- reversed_kafka.bin (fully reversed)")
    print("- word_swapped_kafka.bin (16-bit swapped)")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "interactive":
            fix_byte_order_interactive()
        elif sys.argv[1] == "create_test":
            create_byte_order_test_file()
        elif sys.argv[1].startswith("087f") or len(sys.argv[1]) > 4:
            # Treat as hex data to analyze
            analyze_byte_order(sys.argv[1])
        else:
            print("Usage:")
            print("  python debug_byte_order.py interactive")
            print("  python debug_byte_order.py create_test")
            print("  python debug_byte_order.py 087f0012000000003039")
    else:
        print("Byte Order Debug Tool")
        print("=" * 30)
        print("Usage:")
        print("  python debug_byte_order.py interactive")
        print("  python debug_byte_order.py create_test")
        print("  python debug_byte_order.py <hex_data>")
        print()
        print("This tool helps diagnose byte order issues in Kafka binary data.")
        print("Common issues:")
        print("- Little-endian vs big-endian length prefix")
        print("- Fully reversed byte order")
        print("- Word-swapped data (16-bit boundaries)")
        print()
        print("Example with your data:")
        print("  python debug_byte_order.py 087f0012000000003039")
