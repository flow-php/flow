<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\BlobService\GetBlobProperties;

use Flow\Azure\SDK\BlobService\GetBlobProperties\BlobProperties;
use Flow\Azure\SDK\Tests\Context\BlobServiceContext;
use PHPUnit\Framework\TestCase;

final class BlobPropertiesTest extends TestCase
{
    public function test_the_getters_read_the_content_headers_and_the_metadata(): void
    {
        $properties = new BlobProperties(BlobServiceContext::responseWithHeaders(200, [
            'Content-Type' => 'text/csv',
            'Content-Encoding' => 'gzip',
            'Content-Language' => 'pl',
            'Cache-Control' => 'no-cache',
            'Content-Disposition' => 'attachment',
            'x-ms-meta-Origin' => 'probe',
            'x-ms-meta-team' => 'data',
            'x-ms-version' => '2024-08-04',
        ]));

        static::assertSame('text/csv', $properties->contentType());
        static::assertSame('gzip', $properties->contentEncoding());
        static::assertSame('pl', $properties->contentLanguage());
        static::assertSame('no-cache', $properties->cacheControl());
        static::assertSame('attachment', $properties->contentDisposition());
        static::assertSame(['origin' => 'probe', 'team' => 'data'], $properties->metadata());
    }

    public function test_absent_headers_give_null_and_no_metadata(): void
    {
        $properties = new BlobProperties(BlobServiceContext::response(200, ''));

        static::assertNull($properties->contentType());
        static::assertNull($properties->contentEncoding());
        static::assertNull($properties->contentLanguage());
        static::assertNull($properties->cacheControl());
        static::assertNull($properties->contentDisposition());
        static::assertSame([], $properties->metadata());
    }
}
