<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use Flow\Filesystem\Path;
use PHPUnit\Framework\Attributes\DataProvider;

final class AzureBlobSourceStreamTest extends AzureBlobServiceTestCase
{
    public static function line_lengths() : \Generator
    {
        yield [1];
        yield [7];
        yield [10];
        yield [20];
        yield [30];
        yield [40];
        yield [1024];
    }

    public function test_iterating_through_blob() : void
    {
        $content = <<<'TEXT'
This is some
multi line file
that we are storing on azure blob
TEXT;
        $this->givenFileExists('flow-php', 'file.txt', $content);

        $stream = azure_filesystem($this->blobService('flow-php'))->readFrom(new Path('azure-blob://file.txt'));

        self::assertSame($content, \implode('', \iterator_to_array($stream->iterate())));

        $stream->close();
    }

    public function test_reading_from_blob_by_limit_and_offset() : void
    {
        $content = <<<'TEXT'
This is some
multi line file
that we are storing on azure blob
TEXT;
        $this->givenFileExists('flow-php', 'file.txt', $content);

        $stream = azure_filesystem($this->blobService('flow-php'))->readFrom(new Path('azure-blob://file.txt'));

        self::assertSame($content, $stream->content());

        self::assertSame('This is some', $stream->read(12, 0));
        self::assertSame(12, \strlen($stream->read(12, 0)));
        self::assertSame('multi line file', $stream->read(15, 13));
        self::assertSame(15, \strlen($stream->read(15, 13)));
        self::assertSame('that we are storing on azure blob', $stream->read(33, 29));
        self::assertSame(33, \strlen($stream->read(33, 29)));

        $stream->close();
    }

    #[DataProvider('line_lengths')]
    public function test_reading_lines_from_blob(int $lineLength) : void
    {
        $content = <<<'TEXT'
This is some
multi line file
that we are storing on azure blob
TEXT;
        $this->givenFileExists('flow-php', 'file.txt', $content);

        $stream = azure_filesystem($this->blobService('flow-php'))->readFrom(new Path('azure-blob://file.txt'));

        self::assertSame($content, $stream->content());

        $lines = $stream->readLines(length: $lineLength);
        self::assertSame('This is some', $lines->current());
        $lines->next();
        self::assertSame('multi line file', $lines->current());
        $lines->next();
        self::assertSame('that we are storing on azure blob', $lines->current());
        $lines->next();
        self::assertNull($lines->current());

        $stream->close();
    }
}
