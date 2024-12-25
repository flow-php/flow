<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\DSL\path;

final class AsyncAWSS3DestinationStreamTest extends AsyncAWSS3TestCase
{
    public function test_closing_empty_stream() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());
        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        self::assertTrue($stream->isOpen());
        $stream->close();
        self::assertFalse($stream->isOpen());
    }

    public function test_writing_content_from_resource() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://orders.csv'));
        $stream->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://orders.csv'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://orders.csv'))->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(path('aws-s3://orders.csv'))->content());

        $fs->rm(path('aws-s3://orders.csv'));
    }

    public function test_writing_content_smaller_than_block_size_to_s3() : void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(path('aws-s3://file.txt'))->isFile());
        self::assertFalse($fs->status(path('aws-s3://file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://file.txt'))->content());

        $fs->rm(path('aws-s3://file.txt'));
    }
}
