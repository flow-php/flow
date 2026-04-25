<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Bridge\Azure\Tests\Double\RecordingBlobService;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

final class AzureBlobFilesystemFileFastPathTest extends AzureBlobServiceTestCase
{
    public function test_list_disabled_fast_path_falls_back_to_listing_for_single_file() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-opt-disabled'));
        $fs = azure_filesystem(
            $blobService,
            (new Options())->withFileFastPath(false),
        );

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/orders.csv')));

        self::assertCount(1, $statuses);
        self::assertSame(0, $blobService->getBlobPropertiesCount, 'getBlobProperties must not be issued when fast path is disabled');
        self::assertSame(1, $blobService->listBlobsCount, 'listBlobs must run when fast path is disabled');
    }

    public function test_list_filter_is_applied_on_single_file_fast_path() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-filter'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array(
            $fs->list(path('azure-blob://orders/orders.csv'), new RejectingFilter()),
        );

        self::assertCount(0, $statuses);
        self::assertSame(1, $blobService->getBlobPropertiesCount);
        self::assertSame(0, $blobService->listBlobsCount);
    }

    public function test_list_folder_path_with_trailing_slash_skips_get_blob_properties() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-folder'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/b.csv'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/')));

        self::assertCount(2, $statuses);
        self::assertSame(0, $blobService->getBlobPropertiesCount, 'getBlobProperties must be skipped for paths ending with /');
        self::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_non_existing_single_file_path_falls_back_to_listing() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-missing'));
        $fs = azure_filesystem($blobService);

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://missing/file.csv')));

        self::assertCount(0, $statuses);
        self::assertSame(1, $blobService->getBlobPropertiesCount, 'getBlobProperties is attempted on non-pattern, non-folder path');
        self::assertSame(1, $blobService->listBlobsCount, 'fallback listing runs after null properties');
    }

    public function test_list_pattern_skips_get_blob_properties() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-pattern'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/b.csv'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/*.csv')));

        self::assertCount(2, $statuses);
        self::assertSame(0, $blobService->getBlobPropertiesCount, 'getBlobProperties must not be issued for pattern paths');
        self::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_root_path_skips_get_blob_properties() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-root'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://root.txt'))->append('x')->close();

        $blobService->resetCounters();

        \iterator_to_array($fs->list(path('azure-blob:///')));

        self::assertSame(0, $blobService->getBlobPropertiesCount);
        self::assertSame(1, $blobService->listBlobsCount);
    }

    public function test_list_single_file_does_not_yield_prefix_siblings() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-siblings'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/file.txt'))->append('a')->close();
        $fs->writeTo(path('azure-blob://orders/file.txt.bak'))->append('b')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('azure-blob://orders/file.txt')));

        self::assertCount(1, $statuses);
        self::assertSame('azure-blob://orders/file.txt', $statuses[0]->path->uri());
        self::assertSame(1, $blobService->getBlobPropertiesCount);
        self::assertSame(0, $blobService->listBlobsCount);
    }

    public function test_list_single_file_yields_via_get_blob_properties_only_when_fast_path_enabled() : void
    {
        $blobService = new RecordingBlobService($this->blobService('flow-php-list-opt-enabled'));
        $fs = azure_filesystem($blobService);

        $fs->writeTo(path('azure-blob://orders/orders.csv'))->append('a,b,c')->close();

        $blobService->resetCounters();

        $statuses = \iterator_to_array(
            $fs->list(path('azure-blob://orders/orders.csv'), new OnlyFiles()),
        );

        self::assertCount(1, $statuses);
        self::assertSame('azure-blob://orders/orders.csv', $statuses[0]->path->uri());
        self::assertTrue($statuses[0]->isFile());
        self::assertSame(1, $blobService->getBlobPropertiesCount, 'exactly one getBlobProperties must be issued');
        self::assertSame(0, $blobService->listBlobsCount, 'listBlobs must NOT be issued for single-file fast path');
    }
}
