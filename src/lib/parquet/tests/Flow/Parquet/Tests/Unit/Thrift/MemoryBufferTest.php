<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Thrift;

use Flow\Parquet\Thrift\MemoryBuffer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Thrift\Exception\TTransportException;

final class MemoryBufferTest extends TestCase
{
    public static function read_length_provider(): \Generator
    {
        yield 'read less than available' => [10, 5, 5];
        yield 'read exact amount available' => [10, 10, 10];
        yield 'read more than available' => [10, 15, 10];
        yield 'read single byte' => [10, 1, 1];
        yield 'read zero bytes' => [10, 0, 0];
    }

    public static function write_data_provider(): \Generator
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

    public function test_available_after_write(): void
    {
        $buffer = new MemoryBuffer();

        static::assertSame(0, $buffer->available());

        $buffer->write('hello');
        static::assertSame(5, $buffer->available());

        $buffer->write(' world');
        static::assertSame(11, $buffer->available());
    }

    public function test_available_returns_correct_count(): void
    {
        $buffer = new MemoryBuffer('hello world');

        static::assertSame(11, $buffer->available());

        $buffer->read(5);
        static::assertSame(6, $buffer->available());

        $buffer->read(6);
        static::assertSame(0, $buffer->available());
    }

    public function test_available_with_empty_buffer(): void
    {
        $buffer = new MemoryBuffer();

        static::assertSame(0, $buffer->available());
    }

    public function test_close_method_does_nothing(): void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->close();

        static::assertSame($originalData, $buffer->data());
        static::assertSame($originalAvailable, $buffer->available());
    }

    public function test_constructor_with_binary_data(): void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";
        $buffer = new MemoryBuffer($binaryData);

        static::assertSame($binaryData, $buffer->data());
        static::assertSame(5, $buffer->available());
    }

    public function test_constructor_with_empty_string(): void
    {
        $buffer = new MemoryBuffer();

        static::assertSame('', $buffer->data());
        static::assertSame(0, $buffer->available());
    }

    public function test_constructor_with_initial_data(): void
    {
        $initialData = 'hello world';
        $buffer = new MemoryBuffer($initialData);

        static::assertSame($initialData, $buffer->data());
        static::assertSame(\strlen($initialData), $buffer->available());
    }

    public function test_constructor_with_unicode_data(): void
    {
        $unicodeData = 'Hello 世界 🌟';
        $buffer = new MemoryBuffer($unicodeData);

        static::assertSame($unicodeData, $buffer->data());
        static::assertSame(\strlen($unicodeData), $buffer->available());
    }

    public function test_data_method_returns_current_buffer_content(): void
    {
        $buffer = new MemoryBuffer('initial');

        static::assertSame('initial', $buffer->data());

        $buffer->write(' data');
        static::assertSame('initial data', $buffer->data());
    }

    public function test_data_method_unchanged_after_read(): void
    {
        $buffer = new MemoryBuffer('hello world');

        $buffer->read(5);

        // data() should still return the full buffer content
        static::assertSame('hello world', $buffer->data());
    }

    public function test_is_open_always_returns_true(): void
    {
        $buffer = new MemoryBuffer();
        static::assertTrue($buffer->isOpen());

        $buffer = new MemoryBuffer('some data');
        static::assertTrue($buffer->isOpen());
    }

    public function test_large_data_handling(): void
    {
        $largeData = \str_repeat('A', 10000);
        $buffer = new MemoryBuffer($largeData);

        static::assertSame(10000, $buffer->available());

        $chunk1 = $buffer->read(5000);
        static::assertSame(\str_repeat('A', 5000), $chunk1);
        static::assertSame(5000, $buffer->available());

        $chunk2 = $buffer->read(5000);
        static::assertSame(\str_repeat('A', 5000), $chunk2);
        static::assertSame(0, $buffer->available());
    }

    public function test_open_method_does_nothing(): void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->open();

        static::assertSame($originalData, $buffer->data());
        static::assertSame($originalAvailable, $buffer->available());
    }

    public function test_read_binary_data(): void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";
        $buffer = new MemoryBuffer($binaryData);

        $result = $buffer->read(5);

        static::assertSame($binaryData, $result);
        static::assertSame(0, $buffer->available());
    }

    public function test_read_from_empty_buffer_throws_exception(): void
    {
        $buffer = new MemoryBuffer();

        $this->expectException(TTransportException::class);
        $this->expectExceptionMessage('TMemoryBuffer: Could not read 5 bytes from buffer.');

        $buffer->read(5);
    }

    public function test_read_from_exhausted_buffer_throws_exception(): void
    {
        $buffer = new MemoryBuffer('test');
        $buffer->read(4); // Exhaust the buffer

        $this->expectException(TTransportException::class);
        $this->expectExceptionMessage('TMemoryBuffer: Could not read 3 bytes from buffer.');

        $buffer->read(3);
    }

    public function test_read_full_buffer(): void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(5);

        static::assertSame('hello', $result);
        static::assertSame(0, $buffer->available());
    }

    public function test_read_more_than_available(): void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(10);

        static::assertSame('hello', $result);
        static::assertSame(0, $buffer->available());
    }

    public function test_read_negative_length_behavior(): void
    {
        $buffer = new MemoryBuffer('hello');

        // With substr($data, 0, -1), PHP returns 'hell' (all chars except the last one)
        $result = $buffer->read(-1);

        static::assertSame('hell', $result);
        static::assertSame(6, $buffer->available()); // Position becomes -1, so available = 5 - (-1) = 6
    }

    public function test_read_partial_buffer(): void
    {
        $buffer = new MemoryBuffer('hello world');

        $result = $buffer->read(5);

        static::assertSame('hello', $result);
        static::assertSame(6, $buffer->available());
    }

    public function test_read_sequential_operations(): void
    {
        $buffer = new MemoryBuffer('hello world');

        $first = $buffer->read(5);
        $second = $buffer->read(1);
        $third = $buffer->read(5);

        static::assertSame('hello', $first);
        static::assertSame(' ', $second);
        static::assertSame('world', $third);
        static::assertSame(0, $buffer->available());
    }

    #[DataProvider('read_length_provider')]
    public function test_read_various_lengths(int $dataLength, int $readLength, int $expectedReadLength): void
    {
        $data = \str_repeat('A', $dataLength);
        $buffer = new MemoryBuffer($data);

        $result = $buffer->read($readLength);

        static::assertSame($expectedReadLength, \strlen($result));
        static::assertSame(\str_repeat('A', $expectedReadLength), $result);
    }

    public function test_read_zero_bytes(): void
    {
        $buffer = new MemoryBuffer('hello');

        $result = $buffer->read(0);

        static::assertSame('', $result);
        static::assertSame(5, $buffer->available());
    }

    public function test_write_and_read_integration(): void
    {
        $buffer = new MemoryBuffer();

        $buffer->write('Hello');
        $buffer->write(' ');
        $buffer->write('World');

        static::assertSame('Hello World', $buffer->data());
        static::assertSame(11, $buffer->available());

        $first = $buffer->read(5);
        static::assertSame('Hello', $first);
        static::assertSame(6, $buffer->available());

        $buffer->write('!');
        static::assertSame('Hello World!', $buffer->data());
        static::assertSame(7, $buffer->available());

        $remaining = $buffer->read(7);
        static::assertSame(' World!', $remaining);
        static::assertSame(0, $buffer->available());
    }

    public function test_write_appends_data(): void
    {
        $buffer = new MemoryBuffer('initial');

        $buffer->write(' data');

        static::assertSame('initial data', $buffer->data());
        static::assertSame(12, $buffer->available());
    }

    public function test_write_binary_data(): void
    {
        $buffer = new MemoryBuffer();
        $binaryData = "\x00\x01\x02\x03";

        $buffer->write($binaryData);

        static::assertSame($binaryData, $buffer->data());
        static::assertSame(4, $buffer->available());
    }

    public function test_write_empty_string(): void
    {
        $buffer = new MemoryBuffer('test');
        $originalData = $buffer->data();
        $originalAvailable = $buffer->available();

        $buffer->write('');

        static::assertSame($originalData, $buffer->data());
        static::assertSame($originalAvailable, $buffer->available());
    }

    public function test_write_multiple_times(): void
    {
        $buffer = new MemoryBuffer();

        $buffer->write('Hello');
        $buffer->write(' ');
        $buffer->write('World');

        static::assertSame('Hello World', $buffer->data());
        static::assertSame(11, $buffer->available());
    }

    #[DataProvider('write_data_provider')]
    public function test_write_various_data_types(string $data): void
    {
        $buffer = new MemoryBuffer();

        $buffer->write($data);

        static::assertSame($data, $buffer->data());
        static::assertSame(\strlen($data), $buffer->available());
    }
}
