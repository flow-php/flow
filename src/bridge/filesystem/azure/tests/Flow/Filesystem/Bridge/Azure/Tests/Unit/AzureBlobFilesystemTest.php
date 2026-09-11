<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Exception\InvalidSchemeException;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AzureBlobFilesystemTest extends TestCase
{
    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "file://" is not supported by this protocol. Expected scheme is "azure-blob://"',
        );

        azure_filesystem($this->createStub(BlobServiceInterface::class))->appendTo(path('file:///var/foo.txt'));
    }

    public function test_list_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        iterator_to_array(
            azure_filesystem($this->createStub(BlobServiceInterface::class))->list(path('file:///var/foo.txt')),
        );
    }

    public function test_mv_rejects_mismatched_destination_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->mv(
            path('azure-blob://var/a.txt'),
            path('file:///var/b.txt'),
        );
    }

    public function test_mv_rejects_mismatched_source_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->mv(
            path('file:///var/a.txt'),
            path('azure-blob://var/b.txt'),
        );
    }

    public function test_read_from_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->readFrom(path('file:///var/foo.txt'));
    }

    public function test_rm_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->rm(path('file:///var/foo.txt'));
    }

    public function test_status_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->status(path('file:///var/foo.txt'));
    }

    public function test_supports_only_its_own_scheme(): void
    {
        $filesystem = azure_filesystem($this->createStub(BlobServiceInterface::class));

        static::assertTrue($filesystem->supports(path('azure-blob://container/orders.csv')));
        static::assertFalse($filesystem->supports(path('file:///var/foo.txt')));
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        azure_filesystem($this->createStub(BlobServiceInterface::class))->writeTo(path('file:///var/foo.txt'));
    }
}
