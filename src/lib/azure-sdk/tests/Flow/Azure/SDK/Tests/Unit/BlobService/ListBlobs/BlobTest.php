<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\BlobService\ListBlobs;

use Flow\Azure\SDK\BlobService\ListBlobs\Blob;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class BlobTest extends TestCase
{
    public function test_name_returns_blob_name_from_data(): void
    {
        static::assertSame('my-blob.txt', (new Blob(['Name' => 'my-blob.txt']))->name());
    }

    public function test_name_throws_when_blob_name_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Blob name must be a string');

        (new Blob([]))->name();
    }

    public function test_name_throws_when_blob_name_is_not_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Blob name must be a string');

        (new Blob(['Name' => 123]))->name();
    }

    public function test_size_returns_integer_from_string_content_length(): void
    {
        static::assertSame(4096, (new Blob(['Properties' => ['Content-Length' => '4096']]))->size());
    }

    public function test_size_returns_integer_from_int_content_length(): void
    {
        static::assertSame(4096, (new Blob(['Properties' => ['Content-Length' => 4096]]))->size());
    }

    public function test_size_throws_when_properties_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Blob properties must be an array');

        (new Blob([]))->size();
    }

    public function test_size_throws_when_content_length_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Content-Length must be a string or integer');

        (new Blob(['Properties' => []]))->size();
    }

    public function test_size_throws_when_content_length_wrong_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Content-Length must be a string or integer');

        (new Blob(['Properties' => ['Content-Length' => 1.5]]))->size();
    }

    public function test_last_modified_at_returns_datetime_for_valid_rfc7231_string(): void
    {
        $blob = new Blob(['Properties' => ['Last-Modified' => 'Mon, 25 Dec 2023 12:00:00 GMT']]);

        $lastModified = $blob->lastModifiedAt();

        static::assertNotNull($lastModified);
        static::assertSame('2023-12-25T12:00:00+00:00', $lastModified->format('c'));
    }

    public function test_last_modified_at_returns_null_when_properties_missing(): void
    {
        static::assertNull((new Blob([]))->lastModifiedAt());
    }

    public function test_last_modified_at_returns_null_when_last_modified_missing(): void
    {
        static::assertNull((new Blob(['Properties' => []]))->lastModifiedAt());
    }

    public function test_last_modified_at_returns_null_for_empty_last_modified(): void
    {
        static::assertNull((new Blob(['Properties' => ['Last-Modified' => '']]))->lastModifiedAt());
    }

    public function test_last_modified_at_returns_null_for_unparseable_last_modified(): void
    {
        static::assertNull((new Blob(['Properties' => ['Last-Modified' => 'not-a-date']]))->lastModifiedAt());
    }
}
