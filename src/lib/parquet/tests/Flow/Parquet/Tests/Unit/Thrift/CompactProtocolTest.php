<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Thrift;

use Flow\Parquet\Thrift\{CompactProtocol, MemoryBuffer};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Thrift\Exception\{TProtocolException, TTransportException};
use Thrift\Type\TType;

final class CompactProtocolTest extends TestCase
{
    public static function byte_data() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive' => [42];
        yield 'negative' => [-42];
        yield 'max positive' => [127];
        yield 'max negative' => [-128];
    }

    public static function double_data() : \Generator
    {
        yield 'zero' => [0.0];
        yield 'positive small' => [3.14159];
        yield 'negative small' => [-3.14159];
        yield 'positive large' => [123456789.987654321];
        yield 'negative large' => [-123456789.987654321];
        yield 'very small positive' => [0.000000001];
        yield 'very small negative' => [-0.000000001];
    }

    public static function i16_data() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive small' => [42];
        yield 'negative small' => [-42];
        yield 'positive large' => [12345];
        yield 'negative large' => [-12345];
        yield 'max positive' => [32767];
        yield 'max negative' => [-32768];
    }

    public static function i32_data() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive small' => [42];
        yield 'negative small' => [-42];
        yield 'positive large' => [1234567];
        yield 'negative large' => [-1234567];
        yield 'max positive' => [2147483647];
        yield 'max negative' => [-2147483648];
    }

    public static function i64_data() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive small' => [42];
        yield 'negative small' => [-42];
        yield 'positive large' => [123456789012345];
        yield 'negative large' => [-123456789012345];
        yield 'positive 32bit boundary' => [4294967296];
        yield 'negative 32bit boundary' => [-4294967296];
    }

    public static function list_data() : \Generator
    {
        yield 'empty list' => [TType::STRING, []];
        yield 'string list' => [TType::STRING, ['item1', 'item2', 'item3']];
        yield 'int list' => [TType::I32, [1, 2, 3, -4, -5]];
        yield 'large list' => [TType::I32, range(1, 20)];
    }

    public static function map_data() : \Generator
    {
        yield 'empty map' => [TType::STRING, TType::STRING, []];
        yield 'string to string map' => [TType::STRING, TType::STRING, ['key1' => 'value1', 'key2' => 'value2']];
        yield 'string to int map' => [TType::STRING, TType::I32, ['key1' => 42, 'key2' => -123]];
        yield 'int to string map' => [TType::I32, TType::STRING, [1 => 'value1', 2 => 'value2']];
        yield 'int to int map' => [TType::I32, TType::I32, [1 => 100, 2 => 200]];
    }

    public static function set_data() : \Generator
    {
        yield 'empty set' => [TType::STRING, []];
        yield 'string set' => [TType::STRING, ['item1', 'item2', 'item3']];
        yield 'int set' => [TType::I32, [1, 2, 3, 4, 5]];
    }

    public static function skip_type_data() : \Generator
    {
        yield 'skip bool' => [TType::BOOL];
        yield 'skip byte' => [TType::BYTE];
        yield 'skip i16' => [TType::I16];
        yield 'skip i32' => [TType::I32];
        yield 'skip i64' => [TType::I64];
        yield 'skip double' => [TType::DOUBLE];
        yield 'skip string' => [TType::STRING];
        yield 'skip struct' => [TType::STRUCT];
        yield 'skip map' => [TType::MAP];
        yield 'skip set' => [TType::SET];
        yield 'skip list' => [TType::LST];
    }

    public static function string_data() : \Generator
    {
        yield 'empty string' => [''];
        yield 'simple string' => ['hello'];
        yield 'string with spaces' => ['hello world'];
        yield 'string with special chars' => ['hello@#$%^&*()'];
        yield 'unicode string' => ['こんにちは世界'];
        yield 'long string' => [str_repeat('abc123', 100)];
        yield 'string with null byte' => ["hello\0world"];
    }

    public static function varint_data() : \Generator
    {
        yield 'zero' => [0, "\x00"];
        yield 'small positive' => [127, "\x7F"];
        yield 'medium positive' => [128, "\x80\x01"];
        yield 'large positive' => [16384, "\x80\x80\x01"];
        yield 'very large positive' => [2097152, "\x80\x80\x80\x01"];
    }

    public static function zigzag_encode_decode_data() : \Generator
    {
        yield 'positive number 16 bit' => [123, 16, 246];
        yield 'negative number 16 bit' => [-123, 16, 245];
        yield 'zero 16 bit' => [0, 16, 0];
        yield 'positive number 32 bit' => [123456, 32, 246912];
        yield 'negative number 32 bit' => [-123456, 32, 246911];
        yield 'zero 32 bit' => [0, 32, 0];
        yield 'positive number 64 bit' => [123456789, 64, 246913578];
        yield 'negative number 64 bit' => [-123456789, 64, 246913577];
        yield 'zero 64 bit' => [0, 64, 0];
    }

    public function test_complex_nested_structure() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $protocol->writeStructBegin();

        $protocol->writeFieldBegin('name', TType::STRING, 1);
        $protocol->writeString('test_name');
        $protocol->writeFieldEnd();

        $protocol->writeFieldBegin('numbers', TType::LST, 2);
        $protocol->writeListBegin(TType::I32, 3);
        $protocol->writeI32(1);
        $protocol->writeI32(2);
        $protocol->writeI32(3);
        $protocol->writeListEnd();
        $protocol->writeFieldEnd();

        $protocol->writeFieldBegin('metadata', TType::MAP, 3);
        $protocol->writeMapBegin(TType::STRING, TType::STRING, 2);
        $protocol->writeString('key1');
        $protocol->writeString('value1');
        $protocol->writeString('key2');
        $protocol->writeString('value2');
        $protocol->writeMapEnd();
        $protocol->writeFieldEnd();

        $protocol->writeFieldStop();
        $protocol->writeStructEnd();

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readProtocol->readStructBegin($structName);

        // Read field 1
        $readProtocol->readFieldBegin($name1, $type1, $id1);
        $readProtocol->readString($nameValue);
        $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($name2, $type2, $id2);
        $readProtocol->readListBegin($listType, $listSize);
        $numbers = [];

        for ($i = 0; $i < $listSize; $i++) {
            $readProtocol->readI32($number);
            $numbers[] = $number;
        }
        $readProtocol->readListEnd();
        $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($name3, $type3, $id3);
        $readProtocol->readMapBegin($keyType, $valueType, $mapSize);
        $metadata = [];

        for ($i = 0; $i < $mapSize; $i++) {
            $readProtocol->readString($key);
            $readProtocol->readString($value);
            $metadata[$key] = $value;
        }
        $readProtocol->readMapEnd();
        $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($stopName, $stopType, $stopId);
        $readProtocol->readStructEnd();

        self::assertSame(TType::STRING, $type1);
        self::assertSame(1, $id1);
        self::assertSame('test_name', $nameValue);

        self::assertSame(TType::LST, $type2);
        self::assertSame(2, $id2);
        self::assertSame(TType::I32, $listType);
        self::assertSame(3, $listSize);
        self::assertSame([1, 2, 3], $numbers);

        self::assertSame(TType::MAP, $type3);
        self::assertSame(3, $id3);
        self::assertSame(TType::STRING, $keyType);
        self::assertSame(TType::STRING, $valueType);
        self::assertSame(2, $mapSize);
        self::assertSame(['key1' => 'value1', 'key2' => 'value2'], $metadata);

        self::assertSame(TType::STOP, $stopType);
    }

    public function test_get_transport() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $result = $protocol->getTransport();

        self::assertSame($buffer, $result);
    }

    public function test_get_ttype() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        self::assertSame(TType::BOOL, $protocol->getTType(CompactProtocol::COMPACT_TRUE));
        self::assertSame(TType::BOOL, $protocol->getTType(CompactProtocol::COMPACT_FALSE));
        self::assertSame(TType::BYTE, $protocol->getTType(CompactProtocol::COMPACT_BYTE));
        self::assertSame(TType::I16, $protocol->getTType(CompactProtocol::COMPACT_I16));
        self::assertSame(TType::I32, $protocol->getTType(CompactProtocol::COMPACT_I32));
        self::assertSame(TType::I64, $protocol->getTType(CompactProtocol::COMPACT_I64));
        self::assertSame(TType::DOUBLE, $protocol->getTType(CompactProtocol::COMPACT_DOUBLE));
        self::assertSame(TType::STRING, $protocol->getTType(CompactProtocol::COMPACT_BINARY));
        self::assertSame(TType::STRUCT, $protocol->getTType(CompactProtocol::COMPACT_STRUCT));
        self::assertSame(TType::LST, $protocol->getTType(CompactProtocol::COMPACT_LIST));
        self::assertSame(TType::SET, $protocol->getTType(CompactProtocol::COMPACT_SET));
        self::assertSame(TType::MAP, $protocol->getTType(CompactProtocol::COMPACT_MAP));
        self::assertSame(TType::STOP, $protocol->getTType(CompactProtocol::COMPACT_STOP));
    }

    public function test_protocol_constants() : void
    {
        self::assertSame(0x08, CompactProtocol::COMPACT_BINARY);
        self::assertSame(0x03, CompactProtocol::COMPACT_BYTE);
        self::assertSame(0x07, CompactProtocol::COMPACT_DOUBLE);
        self::assertSame(0x02, CompactProtocol::COMPACT_FALSE);
        self::assertSame(0x04, CompactProtocol::COMPACT_I16);
        self::assertSame(0x05, CompactProtocol::COMPACT_I32);
        self::assertSame(0x06, CompactProtocol::COMPACT_I64);
        self::assertSame(0x09, CompactProtocol::COMPACT_LIST);
        self::assertSame(0x0B, CompactProtocol::COMPACT_MAP);
        self::assertSame(0x0A, CompactProtocol::COMPACT_SET);
        self::assertSame(0x00, CompactProtocol::COMPACT_STOP);
        self::assertSame(0x0C, CompactProtocol::COMPACT_STRUCT);
        self::assertSame(0x01, CompactProtocol::COMPACT_TRUE);
        self::assertSame(0x82, CompactProtocol::PROTOCOL_ID);
        self::assertSame(1, CompactProtocol::VERSION);
    }

    public function test_read_bool_throws_exception_in_invalid_state() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TProtocolException::class);
        $this->expectExceptionMessage('Invalid state in compact protocol');

        $protocol->readBool($bool);
    }

    public function test_read_message_with_bad_protocol_id_throws_exception() : void
    {
        $buffer = new MemoryBuffer("\xFF\x01\x00\x04test");
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TProtocolException::class);
        $this->expectExceptionMessage('Bad protocol id in TCompact message');

        $protocol->readMessageBegin($name, $type, $seqid);
    }

    public function test_read_message_with_bad_version_throws_exception() : void
    {
        $buffer = new MemoryBuffer("\x82\xFF\x00\x04test");
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TProtocolException::class);
        $this->expectExceptionMessage('Bad version in TCompact message');

        $protocol->readMessageBegin($name, $type, $seqid);
    }

    public function test_read_string_empty() : void
    {
        $buffer = new MemoryBuffer("\x00");
        $protocol = new CompactProtocol($buffer);

        $readBytes = $protocol->readString($value);

        self::assertSame(1, $readBytes);
        self::assertSame('', $value);
    }

    public function test_read_ubyte() : void
    {
        $buffer = new MemoryBuffer("\xFF");
        $protocol = new CompactProtocol($buffer);

        $readBytes = $protocol->readUByte($value);

        self::assertSame(1, $readBytes);
        self::assertSame(255, $value);
    }

    public function test_read_varint() : void
    {
        $buffer = new MemoryBuffer("\x80\x01");
        $protocol = new CompactProtocol($buffer);

        $readBytes = $protocol->readVarint($value);

        self::assertSame(2, $readBytes);
        self::assertSame(128, $value);
    }

    public function test_read_zigzag() : void
    {
        $buffer = new MemoryBuffer("\xF6\x01");
        $protocol = new CompactProtocol($buffer);

        $readBytes = $protocol->readZigZag($value);

        self::assertSame(2, $readBytes);
        self::assertSame(123, $value);
    }

    #[DataProvider('skip_type_data')]
    public function test_skip(int $type) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        // Write some test data based on type
        switch ($type) {
            case TType::BOOL:
                // Bool must be in a container or field context for skip to work
                $protocol->writeStructBegin();
                $protocol->writeFieldBegin('boolField', TType::BOOL, 1);
                $protocol->writeBool(true);
                $protocol->writeFieldEnd();
                $protocol->writeFieldStop();
                $protocol->writeStructEnd();

                break;
            case TType::BYTE:
                $protocol->writeByte(42);

                break;
            case TType::I16:
                $protocol->writeI16(1234);

                break;
            case TType::I32:
                $protocol->writeI32(123456);

                break;
            case TType::I64:
                $protocol->writeI64(123456789);

                break;
            case TType::DOUBLE:
                $protocol->writeDouble(3.14159);

                break;
            case TType::STRING:
                $protocol->writeString('test string');

                break;
            case TType::STRUCT:
                $protocol->writeStructBegin();
                $protocol->writeFieldBegin('field1', TType::STRING, 1);
                $protocol->writeString('value1');
                $protocol->writeFieldEnd();
                $protocol->writeFieldStop();
                $protocol->writeStructEnd();

                break;
            case TType::MAP:
                $protocol->writeMapBegin(TType::STRING, TType::I32, 1);
                $protocol->writeString('key1');
                $protocol->writeI32(42);
                $protocol->writeMapEnd();

                break;
            case TType::SET:
                $protocol->writeSetBegin(TType::STRING, 1);
                $protocol->writeString('item1');
                $protocol->writeSetEnd();

                break;
            case TType::LST:
                $protocol->writeListBegin(TType::STRING, 1);
                $protocol->writeString('item1');
                $protocol->writeListEnd();

                break;
        }

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        if ($type === TType::BOOL) {
            $skippedBytes = $readProtocol->skip(TType::STRUCT);
        } else {
            $skippedBytes = $readProtocol->skip($type);
        }

        self::assertGreaterThan(0, $skippedBytes);
        self::assertSame(0, $readBuffer->available());
    }

    public function test_skip_unknown_type_throws_exception() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TProtocolException::class);
        $this->expectExceptionMessage('Unknown field type: 99');
        $this->expectExceptionCode(TProtocolException::INVALID_DATA);

        $protocol->skip(99);
    }

    public function test_transport_exception_on_insufficient_data() : void
    {
        $buffer = new MemoryBuffer('');
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TTransportException::class);

        $protocol->readByte($value);
    }

    #[DataProvider('varint_data')]
    public function test_varint_encoding(int $value, string $expectedBytes) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $result = $protocol->getVarint($value);

        self::assertSame($expectedBytes, $result);
    }

    public function test_write_read_bool_field() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $protocol->writeStructBegin();
        $protocol->writeFieldBegin('boolField', TType::BOOL, 1);
        $writtenBytes = $protocol->writeBool(true);
        $protocol->writeFieldEnd();
        $protocol->writeFieldStop();
        $protocol->writeStructEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readProtocol->readStructBegin($name);
        $readProtocol->readFieldBegin($fieldName, $fieldType, $fieldId);
        $readProtocol->readBool($boolValue);
        $readProtocol->readFieldEnd();
        $readProtocol->readFieldBegin($stopName, $stopType, $stopId);
        $readProtocol->readStructEnd();

        self::assertSame(TType::BOOL, $fieldType);
        self::assertSame(1, $fieldId);
        self::assertTrue($boolValue);
        self::assertSame(TType::STOP, $stopType);
    }

    public function test_write_read_bool_in_container() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        // Simulate container state
        $protocol->writeCollectionBegin(TType::BOOL, 2);
        $writtenBytes = $protocol->writeBool(true);
        $writtenBytes += $protocol->writeBool(false);
        $protocol->writeCollectionEnd();

        self::assertSame(2, $writtenBytes);

        // Reset buffer position for reading
        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readProtocol->readCollectionBegin($type, $size);
        $readProtocol->readBool($bool1);
        $readProtocol->readBool($bool2);
        $readProtocol->readCollectionEnd();

        self::assertSame(1, $bool1);
        self::assertSame(0, $bool2);
    }

    public function test_write_read_bool_throws_exception_in_invalid_state() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $this->expectException(TProtocolException::class);
        $this->expectExceptionMessage('Invalid state in compact protocol');

        $protocol->writeBool(true);
    }

    #[DataProvider('byte_data')]
    public function test_write_read_byte(int $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeByte($value);
        self::assertSame(1, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readByte($readValue);
        self::assertSame(1, $readBytes);
        self::assertSame($value, $readValue);
    }

    #[DataProvider('double_data')]
    public function test_write_read_double(float $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeDouble($value);
        self::assertSame(8, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readDouble($readValue);
        self::assertSame(8, $readBytes);
        self::assertEqualsWithDelta($value, $readValue, 0.000001);
    }

    public function test_write_read_field() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $fieldName = 'testField';
        $fieldType = TType::STRING;
        $fieldId = 1;

        $protocol->writeStructBegin();
        $writtenBytes = $protocol->writeFieldBegin($fieldName, $fieldType, $fieldId);
        $writtenBytes += $protocol->writeString('test value');
        $writtenBytes += $protocol->writeFieldEnd();
        $writtenBytes += $protocol->writeFieldStop();
        $protocol->writeStructEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readProtocol->readStructBegin($name);
        $readBytes = $readProtocol->readFieldBegin($readName, $readType, $readId);
        $readBytes += $readProtocol->readString($readValue);
        $readBytes += $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($stopName, $stopType, $stopId);
        self::assertSame(TType::STOP, $stopType);
        $readProtocol->readStructEnd();

        self::assertSame($fieldType, $readType);
        self::assertSame($fieldId, $readId);
        self::assertSame('test value', $readValue);
    }

    public function test_write_read_field_with_delta_compression() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $protocol->writeStructBegin();

        $protocol->writeFieldBegin('field1', TType::STRING, 10);
        $protocol->writeString('value1');
        $protocol->writeFieldEnd();

        $protocol->writeFieldBegin('field2', TType::STRING, 11);
        $protocol->writeString('value2');
        $protocol->writeFieldEnd();

        $protocol->writeFieldStop();
        $protocol->writeStructEnd();

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readProtocol->readStructBegin($name);

        $readProtocol->readFieldBegin($name1, $type1, $id1);
        $readProtocol->readString($value1);
        $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($name2, $type2, $id2);
        $readProtocol->readString($value2);
        $readProtocol->readFieldEnd();

        $readProtocol->readFieldBegin($stopName, $stopType, $stopId);
        $readProtocol->readStructEnd();

        self::assertSame(10, $id1);
        self::assertSame(11, $id2);
        self::assertSame('value1', $value1);
        self::assertSame('value2', $value2);
        self::assertSame(TType::STOP, $stopType);
    }

    #[DataProvider('i16_data')]
    public function test_write_read_i16(int $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeI16($value);
        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readI16($readValue);
        self::assertSame($writtenBytes, $readBytes);
        self::assertSame($value, $readValue);
    }

    #[DataProvider('i32_data')]
    public function test_write_read_i32(int $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeI32($value);
        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readI32($readValue);
        self::assertSame($writtenBytes, $readBytes);
        self::assertSame($value, $readValue);
    }

    #[DataProvider('i64_data')]
    public function test_write_read_i64(int $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeI64($value);
        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readI64($readValue);
        self::assertSame($writtenBytes, $readBytes);
        self::assertSame($value, $readValue);
    }

    public function test_write_read_large_collection() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $size = 16; // Size > 14 triggers different encoding path
        $writtenBytes = $protocol->writeListBegin(TType::I32, $size);

        for ($i = 0; $i < $size; $i++) {
            $writtenBytes += $protocol->writeI32($i);
        }

        $writtenBytes += $protocol->writeListEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readListBegin($readElementType, $readSize);

        $readList = [];

        for ($i = 0; $i < $readSize; $i++) {
            $readProtocol->readI32($element);
            $readList[] = $element;
        }

        $readBytes += $readProtocol->readListEnd();

        self::assertSame(TType::I32, $readElementType);
        self::assertSame($size, $readSize);
        self::assertSame(range(0, $size - 1), $readList);
    }

    /**
     * @param array<int|string> $listData
     */
    #[DataProvider('list_data')]
    public function test_write_read_list(int $elementType, array $listData) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $size = count($listData);
        $writtenBytes = $protocol->writeListBegin($elementType, $size);

        foreach ($listData as $element) {
            if ($elementType === TType::STRING) {
                $writtenBytes += $protocol->writeString((string) $element);
            } else {
                $writtenBytes += $protocol->writeI32((int) $element);
            }
        }

        $writtenBytes += $protocol->writeListEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readListBegin($readElementType, $readSize);

        $readList = [];

        for ($i = 0; $i < $readSize; $i++) {
            if ($readElementType === TType::STRING) {
                $readProtocol->readString($element);
            } else {
                $readProtocol->readI32($element);
            }
            $readList[] = $element;
        }

        $readBytes += $readProtocol->readListEnd();

        self::assertSame($elementType, $readElementType);
        self::assertSame($size, $readSize);
        self::assertSame($listData, $readList);
    }

    /**
     * @param array<int|string, int|string> $mapData
     */
    #[DataProvider('map_data')]
    public function test_write_read_map(int $keyType, int $valueType, array $mapData) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $size = count($mapData);
        $writtenBytes = $protocol->writeMapBegin($keyType, $valueType, $size);

        foreach ($mapData as $key => $value) {
            if ($keyType === TType::STRING) {
                $writtenBytes += $protocol->writeString((string) $key);
            } else {
                $writtenBytes += $protocol->writeI32((int) $key);
            }

            if ($valueType === TType::STRING) {
                $writtenBytes += $protocol->writeString((string) $value);
            } else {
                $writtenBytes += $protocol->writeI32((int) $value);
            }
        }

        $writtenBytes += $protocol->writeMapEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readMapBegin($readKeyType, $readValueType, $readSize);

        $readMap = [];

        for ($i = 0; $i < $readSize; $i++) {
            if ($readKeyType === TType::STRING) {
                $readProtocol->readString($key);
            } else {
                $readProtocol->readI32($key);
            }

            if ($readValueType === TType::STRING) {
                $readProtocol->readString($value);
            } else {
                $readProtocol->readI32($value);
            }

            $readMap[$key] = $value;
        }

        $readBytes += $readProtocol->readMapEnd();

        if ($size > 0) {
            self::assertSame($keyType, $readKeyType);
            self::assertSame($valueType, $readValueType);
            self::assertSame($mapData, $readMap);
        } else {
            self::assertSame(TType::STOP, $readKeyType);
            self::assertSame(TType::STOP, $readValueType);
            self::assertEmpty($readMap);
        }
        self::assertSame($size, $readSize);
    }

    public function test_write_read_message() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $name = 'testMethod';
        $type = 1;
        $seqid = 42;

        $writtenBytes = $protocol->writeMessageBegin($name, $type, $seqid);
        $writtenBytes += $protocol->writeMessageEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readMessageBegin($readName, $readType, $readSeqid);
        $readBytes += $readProtocol->readMessageEnd();

        self::assertSame($writtenBytes, $readBytes);
        self::assertSame($name, $readName);
        self::assertSame($type, $readType);
        self::assertSame($seqid, $readSeqid);
    }

    /**
     * @param array<int|string> $setData
     */
    #[DataProvider('set_data')]
    public function test_write_read_set(int $elementType, array $setData) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $size = count($setData);
        $writtenBytes = $protocol->writeSetBegin($elementType, $size);

        foreach ($setData as $element) {
            if ($elementType === TType::STRING) {
                $writtenBytes += $protocol->writeString((string) $element);
            } else {
                $writtenBytes += $protocol->writeI32((int) $element);
            }
        }

        $writtenBytes += $protocol->writeSetEnd();

        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readSetBegin($readElementType, $readSize);

        $readSet = [];

        for ($i = 0; $i < $readSize; $i++) {
            if ($readElementType === TType::STRING) {
                $readProtocol->readString($element);
            } else {
                $readProtocol->readI32($element);
            }
            $readSet[] = $element;
        }

        $readBytes += $readProtocol->readSetEnd();

        self::assertSame($elementType, $readElementType);
        self::assertSame($size, $readSize);
        self::assertSame($setData, $readSet);
    }

    #[DataProvider('string_data')]
    public function test_write_read_string(string $value) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeString($value);
        self::assertGreaterThan(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readString($readValue);
        self::assertSame($writtenBytes, $readBytes);
        self::assertSame($value, $readValue);
    }

    public function test_write_read_struct() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeStructBegin();
        $writtenBytes += $protocol->writeStructEnd();

        self::assertSame(0, $writtenBytes);

        $readBuffer = new MemoryBuffer($buffer->data());
        $readProtocol = new CompactProtocol($readBuffer);

        $readBytes = $readProtocol->readStructBegin($name);
        $readBytes += $readProtocol->readStructEnd();

        self::assertSame(0, $readBytes);
        self::assertSame('', $name);
    }

    public function test_write_ubyte() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeUByte(255);

        self::assertSame(1, $writtenBytes);
        self::assertSame("\xFF", $buffer->data());
    }

    public function test_write_varint() : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $writtenBytes = $protocol->writeVarint(128);

        self::assertSame(2, $writtenBytes);
        self::assertSame("\x80\x01", $buffer->data());
    }

    #[DataProvider('zigzag_encode_decode_data')]
    public function test_zigzag_encode_decode(int $value, int $bits, int $expectedZigzag) : void
    {
        $buffer = new MemoryBuffer();
        $protocol = new CompactProtocol($buffer);

        $zigzag = $protocol->toZigZag($value, $bits);
        self::assertSame($expectedZigzag, $zigzag);

        $decoded = $protocol->fromZigZag($zigzag);
        self::assertSame($value, $decoded);
    }
}
