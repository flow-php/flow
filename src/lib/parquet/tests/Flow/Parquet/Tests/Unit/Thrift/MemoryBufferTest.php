<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Thrift;

use Flow\Parquet\Thrift\MemoryBuffer;
use PHPUnit\Framework\TestCase;
use Thrift\Exception\TTransportException;

final class MemoryBufferTest extends TestCase
{
    public static function read_length_provider() : \Generator
    {
        yield 'read less than available' => [10, 5, 5];
        yield 'read exact amount available' => [10, 10, 10];
        yield 'read more than available' => [10, 15, 10];
        yield 'read single byte' => [10, 1, 1];
        yield 'read zero bytes' => [10, 0, 0];
    }

    public static function write_data_provider() : \Generator
    {
        yield 'empty string' => ['', 'empty string'];
        yield 'simple text' => ['hello', 'simple text'];
        yield 'text with spaces' => ['hello world', 'text with spaces'];
        yield 'text with newlines' => ["line1\nline2\nline3", 'text with newlines'];
        yield 'text with tabs' => ["col1\tcol2\tcol3", 'text with tabs'];
        yield 'binary data' => ["\x00\x01\x02\x03\xFF", 'binary data'];
        yield 'unicode characters' => ['Hello 世界 🌟', 'unicode characters'];
        yield 'json data' => ['{"key": "value", "number": 123}', 'json data'];
        yield 'xml data' => ['<root><item>value</item></root>', 'xml data'];
        yield 'single character' => ['A', 'single character'];
        yield 'repeated characters' => [\str_repeat('X', 100), 'repeated characters'];
    }

    public function test_available_after_write() : void
    {
        $buffer = new MemoryBuffer();

        self::assertSame(0, $buffer->available());

        $buffer->write('hello');
        self::assertSame(5, $buffer->available());

        $buffer->write(' world');
        self::assertSame(11, $buffer->available());
    }

    public function test_available_returns_correct_count() : void
    {
        $buffer = new MemoryBuffer('hello world');

        self::assertSame(11, $buffer->available());

        $buffer->read(5);
        self::assertSame(6, $buffer->available());

        $buffer->read(6);
        self::assertSame(0, $buffer->available());
    }

    public function test_available_with_empty_buffer() : void
    {
        $buffer = new MemoryBuffer();

        self::assertSame(0, $buffer->available());
    }

    public function test_close_method_does_nothing() : void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->close();

        self::assertSame($originalData, $buffer->data());
        self::assertSame($originalAvailable, $buffer->available());
    }

    public function test_constructor_with_binary_data() : void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";
        $buffer = new MemoryBuffer($binaryData);

        self::assertSame($binaryData, $buffer->data());
        self::assertSame(5, $buffer->available());
    }

    public function test_constructor_with_empty_string() : void
    {
        $buffer = new MemoryBuffer();

        self::assertSame('', $buffer->data());
        self::assertSame(0, $buffer->available());
    }

    public function test_constructor_with_initial_data() : void
    {
        $initialData = 'hello world';
        $buffer = new MemoryBuffer($initialData);

        self::assertSame($initialData, $buffer->data());
        self::assertSame(\strlen($initialData), $buffer->available());
    }

    public function test_constructor_with_unicode_data() : void
    {
        $unicodeData = 'Hello 世界 🌟';
        $buffer = new MemoryBuffer($unicodeData);

        self::assertSame($unicodeData, $buffer->data());
        self::assertSame(\strlen($unicodeData), $buffer->available());
    }

    public function test_data_method_returns_current_buffer_content() : void
    {
        $buffer = new MemoryBuffer('initial');

        self::assertSame('initial', $buffer->data());

        $buffer->write(' data');
        self::assertSame('initial data', $buffer->data());
    }

    public function test_data_method_unchanged_after_read() : void
    {
        $buffer = new MemoryBuffer('hello world');

        $buffer->read(5);

        // data() should still return the full buffer content
        self::assertSame('hello world', $buffer->data());
    }

    public function test_is_open_always_returns_true() : void
    {
        $buffer = new MemoryBuffer();
        self::assertTrue($buffer->isOpen());

        $buffer = new MemoryBuffer('some data');
        self::assertTrue($buffer->isOpen());
    }

    public function test_large_data_handling() : void
    {
        $largeData = \str_repeat('A', 10000);
        $buffer = new MemoryBuffer($largeData);

        self::assertSame(10000, $buffer->available());

        $chunk1 = $buffer->read(5000);
        self::assertSame(\str_repeat('A', 5000), $chunk1);
        self::assertSame(5000, $buffer->available());

        $chunk2 = $buffer->read(5000);
        self::assertSame(\str_repeat('A', 5000), $chunk2);
        self::assertSame(0, $buffer->available());
    }

    public function test_open_method_does_nothing() : void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->open();

        self::assertSame($originalData, $buffer->data());
        self::assertSame($originalAvailable, $buffer->available());
    }

    public function test_read_binary_data() : void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";
        $buffer = new MemoryBuffer($binaryData);

        $result = $buffer->read(5);

        self::assertSame($binaryData, $result);
        self::assertSame(0, $buffer->available());
    }

    public function test_read_from_empty_buffer_throws_exception() : void
    {
        $buffer = new MemoryBuffer();

        $this->expectException(TTransportException::class);
        $this->expectExceptionMessage('TMemoryBuffer: Could not read 5 bytes from buffer.');

        $buffer->read(5);
    }

    public function test_read_from_exhausted_buffer_throws_exception() : void
    {
        $buffer = new MemoryBuffer('test');
        $buffer->read(4); // Exhaust the buffer

        $this->expectException(TTransportException::class);
        $this->expectExceptionMessage('TMemoryBuffer: Could not read 3 bytes from buffer.');

        $buffer->read(3);
    }

    public function test_read_full_buffer() : void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(5);

        self::assertSame('hello', $result);
        self::assertSame(0, $buffer->available());
    }

    public function test_read_more_than_available() : void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(10);

        self::assertSame('hello', $result);
        self::assertSame(0, $buffer->available());
    }

    public function test_read_negative_length_behavior() : void
    {
        $buffer = new MemoryBuffer('hello');

        // With substr($data, 0, -1), PHP returns 'hell' (all chars except the last one)
        $result = $buffer->read(-1);

        self::assertSame('hell', $result);
        self::assertSame(6, $buffer->available()); // Position becomes -1, so available = 5 - (-1) = 6
    }

    public function test_read_partial_buffer() : void
    {
        $buffer = new MemoryBuffer('hello world');

        $result = $buffer->read(5);

        self::assertSame('hello', $result);
        self::assertSame(6, $buffer->available());
    }

    public function test_read_sequential_operations() : void
    {
        $buffer = new MemoryBuffer('hello world');

        $first = $buffer->read(5);
        $second = $buffer->read(1);
        $third = $buffer->read(5);

        self::assertSame('hello', $first);
        self::assertSame(' ', $second);
        self::assertSame('world', $third);
        self::assertSame(0, $buffer->available());
    }

    /**
     * @dataProvider read_length_provider
     */
    public function test_read_various_lengths(int $dataLength, int $readLength, int $expectedReadLength) : void
    {
        $data = \str_repeat('A', $dataLength);
        $buffer = new MemoryBuffer($data);

        $result = $buffer->read($readLength);

        self::assertSame($expectedReadLength, \strlen($result));
        self::assertSame(\str_repeat('A', $expectedReadLength), $result);
    }

    public function test_read_zero_bytes() : void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(0);

        self::assertSame('', $result);
        self::assertSame(5, $buffer->available());
    }

    public function test_write_and_read_integration() : void
    {
        $buffer = new MemoryBuffer();

        $buffer->write('Hello');
        $buffer->write(' ');
        $buffer->write('World');

        self::assertSame('Hello World', $buffer->data());
        self::assertSame(11, $buffer->available());

        $first = $buffer->read(5);
        self::assertSame('Hello', $first);
        self::assertSame(6, $buffer->available());

        $buffer->write('!');
        self::assertSame('Hello World!', $buffer->data());
        self::assertSame(7, $buffer->available());

        $remaining = $buffer->read(7);
        self::assertSame(' World!', $remaining);
        self::assertSame(0, $buffer->available());
    }

    public function test_write_appends_data() : void
    {
        $buffer = new MemoryBuffer('initial');

        $buffer->write(' data');

        self::assertSame('initial data', $buffer->data());
        self::assertSame(12, $buffer->available());
    }

    public function test_write_binary_data() : void
    {
        $buffer = new MemoryBuffer();
        $binaryData = "\x00\x01\x02\x03";

        $buffer->write($binaryData);

        self::assertSame($binaryData, $buffer->data());
        self::assertSame(4, $buffer->available());
    }

    public function test_write_empty_string() : void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->write('');

        self::assertSame($originalData, $buffer->data());
        self::assertSame($originalAvailable, $buffer->available());
    }

    public function test_write_multiple_times() : void
    {
        $buffer = new MemoryBuffer();

        $buffer->write('Hello');
        $buffer->write(' ');
        $buffer->write('World');

        self::assertSame('Hello World', $buffer->data());
        self::assertSame(11, $buffer->available());
    }

    /**
     * @dataProvider write_data_provider
     */
    public function test_write_various_data_types(string $data) : void
    {
        $buffer = new MemoryBuffer();

        $buffer->write($data);

        self::assertSame($data, $buffer->data());
        self::assertSame(\strlen($data), $buffer->available());
    }
}
