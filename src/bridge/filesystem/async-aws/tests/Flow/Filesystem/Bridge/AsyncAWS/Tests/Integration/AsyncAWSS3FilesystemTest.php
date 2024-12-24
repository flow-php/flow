<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_filesystem, aws_s3_path};

final class AsyncAWSS3FilesystemTest extends AsyncAWSS3TestCase
{
    public function test_writing_to_aws_s3storage() : void
    {
        $fs = aws_s3_filesystem($this->s3Client());

        $stream = $fs->writeTo(aws_s3_path($this->bucket(), 'file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(aws_s3_path($this->bucket(), 'file.txt'))->isFile());
        self::assertFalse($fs->status(aws_s3_path($this->bucket(), 'file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(aws_s3_path($this->bucket(), 'file.txt'))->content());

        $fs->rm(aws_s3_path($this->bucket(), 'file.txt'));
    }
}
