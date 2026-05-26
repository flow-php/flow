<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function file_get_contents;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;

final class AsyncAWSS3DestinationStreamTest extends AsyncAWSS3TestCase
{
    public function test_closing_empty_stream(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());
        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        static::assertTrue($stream->isOpen());
        $stream->close();
        static::assertFalse($stream->isOpen());
    }

    public function test_writing_content_from_resource(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://orders.csv'));
        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertNotFalse($resource);
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path('aws-s3://orders.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(
            file_get_contents(__DIR__ . '/Fixtures/orders.csv'),
            $fs->readFrom(path('aws-s3://orders.csv'))->content(),
        );

        $fs->rm(path('aws-s3://orders.csv'));
    }

    public function test_writing_content_smaller_than_block_size_to_s3(): void
    {
        $fs = aws_s3_filesystem($this->bucket(), $this->s3Client());

        $stream = $fs->writeTo(path('aws-s3://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path('aws-s3://file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame('Hello, World!', $fs->readFrom(path('aws-s3://file.txt'))->content());

        $fs->rm(path('aws-s3://file.txt'));
    }
}
