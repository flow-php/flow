<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Bridge\Azure\Tests\Double\RecordingBlobService;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;

final class AzureBlobFilesystemFileFastPathTest extends AzureBlobServiceTestCase
{
    public function test_list_disabled_fast_path_falls_back_to_listing_for_single_file(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-opt-disabled'));
        $fs = azure_filesystem($blobService, (new Options())->withFileFastPath(false));

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/orders.csv')));

        static::assertCount(1, $statuses);
        static::assertSame(
            0,
            $blobService->getBlobPropertiesCount,
            'getBlobProperties must not be issued when fast path is disabled',
        );
        static::assertSame(1, $blobService->listBlobsCount, 'listBlobs must run when fast path is disabled');
    }

    public function test_list_filter_is_applied_on_single_file_fast_path(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-filter'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/orders.csv'), new RejectingFilter()));

        static::assertCount(0, $statuses);
        static::assertSame(1, $blobService->getBlobPropertiesCount);
        static::assertSame(0, $blobService->listBlobsCount);
    }

    public function test_list_folder_path_with_trailing_slash_skips_get_blob_properties(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-folder'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/b.csv'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/')));

        static::assertCount(2, $statuses);
        static::assertSame(
            0,
            $blobService->getBlobPropertiesCount,
            'getBlobProperties must be skipped for paths ending with /',
        );
        static::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_non_existing_single_file_path_falls_back_to_listing(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-missing'));
        $fs = azure_filesystem($blobService);

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://missing/file.csv')));

        static::assertCount(0, $statuses);
        static::assertSame(
            1,
            $blobService->getBlobPropertiesCount,
            'getBlobProperties is attempted on non-pattern, non-folder path',
        );
        static::assertSame(1, $blobService->listBlobsCount, 'fallback listing runs after null properties');
    }

    public function test_list_pattern_skips_get_blob_properties(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-pattern'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/b.csv'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/*.csv')));

        static::assertCount(2, $statuses);
        static::assertSame(
            0,
            $blobService->getBlobPropertiesCount,
            'getBlobProperties must not be issued for pattern paths',
        );
        static::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_root_path_skips_get_blob_properties(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-root'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://root.txt'))->append('x')->close();

        $blobService->resetCounters();

        \iterator_to_array($fs->list(path('azure-blob:///')));

        static::assertSame(0, $blobService->getBlobPropertiesCount);
        static::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_single_file_does_not_yield_prefix_siblings(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-siblings'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/file.txt'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/file.txt.bak'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/file.txt')));

        static::assertCount(1, $statuses);
        static::assertSame('azure-blob://orders/file.txt', $statuses[0]->path->uri());
        static::assertSame(1, $blobService->getBlobPropertiesCount);
        static::assertSame(0, $blobService->listBlobsCount);
    }

    public function test_list_single_file_yields_via_get_blob_properties_only_when_fast_path_enabled(): void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-opt-enabled'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b,c')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/orders.csv'), new OnlyFiles()));

        static::assertCount(1, $statuses);
        static::assertSame('azure-blob://orders/orders.csv', $statuses[0]->path->uri());
        static::assertTrue($statuses[0]->isFile());
        static::assertSame(1, $blobService->getBlobPropertiesCount, 'exactly one getBlobProperties must be issued');
        static::assertSame(0, $blobService->listBlobsCount, 'listBlobs must NOT be issued for single-file fast path');
    }
}
