<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\DSL\path;

final class AsyncAWSS3SourceStreamTest extends AsyncAWSS3TestCase
{
    public static function line_lengths(): \Generator
    {
        yield [1];
        yield [7];
        yield [10];
        yield [20];
        yield [30];
        yield [40];
        yield [1024];
    }

    public function test_iterating_through_blob(): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(path('aws-s3://file.txt'), $content);

        $stream = aws_s3_filesystem($this->bucket(), $this->s3Client())->readFrom(path('aws-s3://file.txt'));

        static::assertSame($content, \implode('', \iterator_to_array($stream->iterate())));

        $stream->close();
    }

    public function test_reading_from_blob_by_limit_and_offset(): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(path('aws-s3://file.txt'), $content);

        $stream = aws_s3_filesystem($this->bucket(), $this->s3Client())->readFrom(path('aws-s3://file.txt'));

        static::assertSame($content, $stream->content());

        static::assertSame('This is some', $stream->read(12, 0));
        static::assertSame(12, \strlen($stream->read(12, 0)));
        static::assertSame('multi line file', $stream->read(15, 13));
        static::assertSame(15, \strlen($stream->read(15, 13)));
        static::assertSame('that we are storing on azure blob', $stream->read(33, 29));
        static::assertSame(33, \strlen($stream->read(33, 29)));

        $stream->close();
    }

    #[DataProvider('line_lengths')]
    public function test_reading_lines_from_blob(int $lineLength): void
    {
        $content = <<<'TEXT'
            This is some
            multi line file
            that we are storing on azure blob
            TEXT;
        $this->givenFileExists(path('aws-s3://file.txt'), $content);

        $stream = aws_s3_filesystem($this->bucket(), $this->s3Client())->readFrom(path('aws-s3://file.txt'));

        static::assertSame($content, $stream->content());

        $lines = $stream->readLines(length: $lineLength > 0 ? $lineLength : null);
        static::assertSame('This is some', $lines->current());
        $lines->next();
        static::assertSame('multi line file', $lines->current());
        $lines->next();
        static::assertSame('that we are storing on azure blob', $lines->current());
        $lines->next();
        static::assertNull($lines->current());

        $stream->close();
    }

    public function test_reading_lines_with_falsy_values_preserves_all_lines(): void
    {
        $content = "header\n0\n\nvalue\n0\nlast";
        $this->givenFileExists(path('aws-s3://falsy.csv'), $content);

        $stream = aws_s3_filesystem($this->bucket(), $this->s3Client())->readFrom(path('aws-s3://falsy.csv'));

        $lines = \iterator_to_array($stream->readLines());

        static::assertSame(['header', '0', '', 'value', '0', 'last'], $lines);

        $stream->close();
    }
}
