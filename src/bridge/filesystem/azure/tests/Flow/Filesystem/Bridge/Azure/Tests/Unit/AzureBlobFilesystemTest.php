<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Azure\SDK\Exception\AzureException;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Flow\Filesystem\Bridge\Azure\Tests\Mother\BlobServiceMother;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Types\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function in_array;
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

    public function test_a_configured_filesystem_round_trips_its_configuration(): void
    {
        $filesystem = azure_filesystem([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'c2VjcmV0',
        ], protocol: 'blobs');
        $configuration = $filesystem->normalize();

        static::assertNotNull($configuration);
        static::assertSame('blobs', $configuration['protocol']);
        static::assertSame('c', $configuration['blob_service']['container']);
        static::assertSame($configuration, AzureBlobFilesystem::fromArray($configuration)->normalize());
    }

    public function test_a_filesystem_over_a_live_service_has_no_configuration(): void
    {
        static::assertNull(azure_filesystem($this->createStub(BlobServiceInterface::class))->normalize());
    }

    public function test_a_configuration_without_a_protocol_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        AzureBlobFilesystem::fromArray(['blob_service' => [], 'options' => []]);
    }

    public function test_mv_never_deletes_the_source_of_a_copy_that_is_not_finished(): void
    {
        $calls = [];

        try {
            azure_filesystem(BlobServiceMother::answering([
                'GetBlobProperties' => [200, ['Content-Length' => '12']],
                'CopyBlob' => [202, ['x-ms-copy-status' => 'pending']],
                'DeleteBlob' => [202, []],
            ], $calls), azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024))->mv(
                path('azure-blob://a.txt'),
                path('azure-blob://b.txt'),
            );

            static::fail('mv() of a pending copy must throw');
        } catch (AzureException) {
        }

        static::assertFalse(in_array('DeleteBlob', $calls, true));
    }

    public function test_mv_above_copy_part_size_copies_ranged_blocks_then_commits_then_deletes(): void
    {
        $calls = [];

        azure_filesystem(BlobServiceMother::answering([
            'GetBlobProperties' => [200, ['Content-Length' => '12582912']],
            'PutBlockFromUrl' => [201, []],
            'PutBlockList' => [201, []],
            'DeleteBlob' => [202, []],
        ], $calls), azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024))->mv(
            path('azure-blob://a.txt'),
            path('azure-blob://b.txt'),
        );

        static::assertSame(
            [
                'GetBlobProperties',
                'PutBlockFromUrl bytes=0-5242879',
                'PutBlockFromUrl bytes=5242880-10485759',
                'PutBlockFromUrl bytes=10485760-12582911',
                'PutBlockList',
                'DeleteBlob',
            ],
            $calls,
        );
    }

    public function test_mv_keeps_the_source_when_a_block_fails(): void
    {
        $calls = [];

        try {
            azure_filesystem(BlobServiceMother::answering([
                'GetBlobProperties' => [200, ['Content-Length' => '12582912']],
                'PutBlockFromUrl' => [201, []],
                'PutBlockFromUrl#2' => [500, []],
                'PutBlockList' => [201, []],
                'DeleteBlob' => [202, []],
            ], $calls), azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024))->mv(
                path('azure-blob://a.txt'),
                path('azure-blob://b.txt'),
            );

            static::fail('mv() with a failed block must throw');
        } catch (AzureException) {
        }

        static::assertSame(
            ['GetBlobProperties', 'PutBlockFromUrl bytes=0-5242879', 'PutBlockFromUrl bytes=5242880-10485759'],
            $calls,
        );
    }

    public function test_mv_in_blocks_carries_the_source_properties(): void
    {
        $calls = [];
        $putBlockListHeaders = [];

        azure_filesystem(
            BlobServiceMother::answering(
                [
                    'GetBlobProperties' => [
                        200,
                        [
                            'Content-Length' => '12582912',
                            'Content-Type' => 'text/csv',
                            'Content-Encoding' => 'gzip',
                            'Content-Language' => 'pl',
                            'Cache-Control' => 'no-cache',
                            'Content-Disposition' => 'attachment',
                            'x-ms-meta-origin' => 'probe',
                        ],
                    ],
                    'PutBlockFromUrl' => [201, []],
                    'PutBlockList' => [201, []],
                    'DeleteBlob' => [202, []],
                ],
                $calls,
                $putBlockListHeaders,
            ),
            azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->mv(path('azure-blob://a.txt'), path('azure-blob://b.txt'));

        static::assertCount(1, $putBlockListHeaders);
        static::assertSame('text/csv', $putBlockListHeaders[0]['x-ms-blob-content-type'] ?? null);
        static::assertSame('gzip', $putBlockListHeaders[0]['x-ms-blob-content-encoding'] ?? null);
        static::assertSame('pl', $putBlockListHeaders[0]['x-ms-blob-content-language'] ?? null);
        static::assertSame('no-cache', $putBlockListHeaders[0]['x-ms-blob-cache-control'] ?? null);
        static::assertSame('attachment', $putBlockListHeaders[0]['x-ms-blob-content-disposition'] ?? null);
        static::assertSame('probe', $putBlockListHeaders[0]['x-ms-meta-origin'] ?? null);
    }

    public function test_mv_to_the_same_path_sends_no_request(): void
    {
        $calls = [];

        static::assertTrue(azure_filesystem(
            BlobServiceMother::answering([], $calls),
            azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->mv(path('azure-blob://a.txt'), path('azure-blob://a.txt')));
        static::assertSame([], $calls);
    }

    public function test_mv_of_a_missing_source_copies_nothing(): void
    {
        $calls = [];

        try {
            azure_filesystem(BlobServiceMother::answering([
                'GetBlobProperties' => [404, []],
            ], $calls), azure_filesystem_options()->withCopyPartSize(5 * 1024 * 1024))->mv(
                path('azure-blob://missing.txt'),
                path('azure-blob://b.txt'),
            );

            static::fail('mv() of a missing source must throw');
        } catch (RuntimeException $exception) {
            static::assertSame(
                'Cannot move "azure-blob://missing.txt" to "azure-blob://b.txt", because "azure-blob://missing.txt" does not exist',
                $exception->getMessage(),
            );
        }

        static::assertSame(['GetBlobProperties'], $calls);
    }
}
