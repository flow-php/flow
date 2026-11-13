<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\BlobService\PutBlockBlobBlockList;

use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use PHPUnit\Framework\TestCase;

use function array_key_exists;

final class PutBlockBlobBlockListOptionsTest extends TestCase
{
    public function test_if_none_match_header_is_absent_by_default(): void
    {
        static::assertFalse(array_key_exists('If-None-Match', (new PutBlockBlobBlockListOptions())->toHeaders()));
    }

    public function test_if_none_match_header_is_sent_when_set(): void
    {
        static::assertSame(
            '*',
            (new PutBlockBlobBlockListOptions())
                ->withIfNoneMatch('*')
                ->toHeaders()['If-None-Match'],
        );
    }

    public function test_the_blob_properties_become_headers(): void
    {
        $headers = (new PutBlockBlobBlockListOptions())
            ->withContentType('text/csv')
            ->withContentEncoding('gzip')
            ->withContentLanguage('pl')
            ->withCacheControl('no-cache')
            ->withContentDisposition('attachment')
            ->withMetadata(['origin' => 'probe'])
            ->toHeaders();

        static::assertSame('text/csv', $headers['x-ms-blob-content-type'] ?? null);
        static::assertSame('gzip', $headers['x-ms-blob-content-encoding'] ?? null);
        static::assertSame('pl', $headers['x-ms-blob-content-language'] ?? null);
        static::assertSame('no-cache', $headers['x-ms-blob-cache-control'] ?? null);
        static::assertSame('attachment', $headers['x-ms-blob-content-disposition'] ?? null);
        static::assertSame('probe', $headers['x-ms-meta-origin'] ?? null);
    }

    public function test_unset_blob_properties_send_no_headers(): void
    {
        $headers = (new PutBlockBlobBlockListOptions())->toHeaders();

        static::assertFalse(array_key_exists('x-ms-blob-content-type', $headers));
        static::assertFalse(array_key_exists('x-ms-blob-content-encoding', $headers));
        static::assertFalse(array_key_exists('x-ms-blob-content-language', $headers));
        static::assertFalse(array_key_exists('x-ms-blob-cache-control', $headers));
        static::assertFalse(array_key_exists('x-ms-blob-content-disposition', $headers));
    }

    public function test_null_blob_properties_send_no_headers(): void
    {
        static::assertSame(
            (new PutBlockBlobBlockListOptions())->toHeaders(),
            (new PutBlockBlobBlockListOptions())
                ->withContentType(null)
                ->withContentEncoding(null)
                ->withContentLanguage(null)
                ->withCacheControl(null)
                ->withContentDisposition(null)
                ->withMetadata([])
                ->toHeaders(),
        );
    }
}
