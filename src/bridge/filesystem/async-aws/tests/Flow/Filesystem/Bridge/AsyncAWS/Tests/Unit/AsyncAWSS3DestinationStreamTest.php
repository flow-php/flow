<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DestinationStream;
use Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother\S3ClientMother;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

use function end;
use function Flow\Filesystem\DSL\path;

final class AsyncAWSS3DestinationStreamTest extends TestCase
{
    public function test_exclusive_stream_aborts_and_asks_to_create_again_on_a_conflicting_write(): void
    {
        $calls = [];
        $stream = AsyncAWSS3DestinationStream::openExclusive(
            S3ClientMother::completingWith(409, $calls),
            'bucket',
            path('aws-s3://claim.txt'),
        );
        $stream->append('second');

        try {
            $stream->close();
            static::fail('Expected the conflicting write to be refused');
        } catch (FileExistsException) {
            static::fail('A conflicting write is not an existing file');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('call create() again', $e->getMessage());
        }

        static::assertSame('DELETE uploadId=upload-1', end($calls));
    }

    public function test_exclusive_stream_aborts_and_refuses_an_existing_key(): void
    {
        $calls = [];
        $stream = AsyncAWSS3DestinationStream::openExclusive(
            S3ClientMother::completingWith(412, $calls),
            'bucket',
            path('aws-s3://claim.txt'),
        );
        $stream->append('second');

        try {
            $stream->close();
            static::fail('Expected the existing key to be refused');
        } catch (FileExistsException) {
        }

        static::assertSame('DELETE uploadId=upload-1', end($calls));
    }
}
