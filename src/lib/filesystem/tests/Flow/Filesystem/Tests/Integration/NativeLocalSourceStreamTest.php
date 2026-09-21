<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DataProviderExternal;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function implode;
use function iterator_to_array;
use function strlen;

final class NativeLocalSourceStreamTest extends NativeLocalFilesystemTestCase
{
    /**
     * @return \Generator<int, array{int<1, max>}>
     */
    public static function line_lengths(): Generator
    {
        yield [1];
        yield [7];
        yield [10];
        yield [20];
        yield [30];
        yield [40];
        yield [1024];
    }

    public function test_closing_a_stream_while_its_lines_are_being_iterated(): void
    {
        $this->givenFileExists(__DIR__ . '/var/file.txt', "x\ny\nz");

        $stream = native_local_filesystem()->readFrom(path(__DIR__ . '/var/file.txt'));
        $lines = $stream->readLines();

        static::assertSame('x', $lines->current());

        $stream->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot read from closed stream');

        $lines->next();
    }

    public function test_closing_a_stream_while_it_is_being_iterated(): void
    {
        $this->givenFileExists(__DIR__ . '/var/file.txt', 'xyz');

        $stream = native_local_filesystem()->readFrom(path(__DIR__ . '/var/file.txt'));
        $chunks = $stream->iterate();

        static::assertSame('x', $chunks->current());

        $stream->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot read from closed stream');

        $chunks->next();
    }

    public function test_iterating_through_blob(): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(__DIR__ . '/var/file.txt', $content);

        $stream = (new NativeLocalFilesystem())->readFrom(path(__DIR__ . '/var/file.txt'));

        static::assertSame($content, implode('', iterator_to_array($stream->iterate())));

        $stream->close();
    }

    public function test_reading_from_blob_by_limit_and_offset(): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(__DIR__ . '/var/file.txt', $content);

        $stream = (new NativeLocalFilesystem())->readFrom(path(__DIR__ . '/var/file.txt'));

        static::assertSame($content, $stream->content());

        static::assertSame('This is some', $stream->read(12, 0));
        static::assertSame(12, strlen($stream->read(12, 0)));
        static::assertSame('multi line file', $stream->read(15, 13));
        static::assertSame(15, strlen($stream->read(15, 13)));
        static::assertSame('that we are storing on azure blob', $stream->read(33, 29));
        static::assertSame(33, strlen($stream->read(33, 29)));

        $stream->close();
    }

    /**
     * @param int<1, max> $lineLength
     */
    #[DataProvider('line_lengths')]
    public function test_reading_lines_from_file(int $lineLength): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(__DIR__ . '/var/file.txt', $content);

        $stream = (new NativeLocalFilesystem())->readFrom(path(__DIR__ . '/var/file.txt'));

        static::assertSame($content, $stream->content());

        $lines = $stream->readLines(length: $lineLength);
        static::assertSame('This is some', $lines->current());
        $lines->next();
        static::assertSame('multi line file', $lines->current());
        $lines->next();
        static::assertSame('that we are storing on azure blob', $lines->current());
        $lines->next();
        static::assertNull($lines->current());

        $stream->close();
    }

    public function test_read_lines_from_a_closed_stream_throws(): void
    {
        $this->givenFileExists(__DIR__ . '/var/file.txt', "x\ny");

        $stream = native_local_filesystem()->readFrom(path(__DIR__ . '/var/file.txt'));
        $stream->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot read from closed stream');

        $stream->readLines()->current();
    }

    /**
     * @param non-empty-string $separator
     * @param null|int<1, max> $length
     * @param list<string> $expected
     */
    #[DataProviderExternal(ReadLinesContext::class, 'cases')]
    public function test_read_lines_conforms_to_the_source_stream_contract(
        string $content,
        string $separator,
        ?int $length,
        array $expected,
    ): void {
        $this->givenFileExists(__DIR__ . '/var/file.txt', $content);

        $stream = native_local_filesystem()->readFrom(path(__DIR__ . '/var/file.txt'));

        static::assertSame($expected, iterator_to_array($stream->readLines($separator, $length)));

        $stream->close();
    }
}
