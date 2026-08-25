<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\FileListing;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\OperatingSystem;

use function count;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function scandir;

final class FileListingTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    public function test_filter_is_applied(): void
    {
        $this->givenFileExists(__DIR__ . '/var/filter/orders.csv', "id\n1\n");
        $this->givenFileExists(__DIR__ . '/var/filter/nested/orders.csv', "id\n2\n");

        $listing = new FileListing(new NativeLocalFilesystem());
        $tree = path(__DIR__ . '/var/filter/**/*');

        static::assertCount(2, iterator_to_array($listing->list($tree, new OnlyFiles()), false));
        static::assertCount(3, iterator_to_array($listing->list($tree, new KeepAll()), false));
    }

    public function test_glob_without_placeholders_attaches_the_paths_own_partitions(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://hive/date=2024-01-01/orders.csv'));
        $stream->append("id\n1\n");
        $stream->close();

        $statuses = iterator_to_array(
            (new FileListing($filesystem))->list(path('memory://hive/**/*.csv'), new KeepAll()),
            false,
        );

        static::assertCount(1, $statuses);
        static::assertSame('2024-01-01', $statuses[0]->path->partitions()->get('date')->value);
    }

    public function test_listing_a_directory_yields_file_statuses(): void
    {
        $filesystem = memory_filesystem();

        foreach (['memory://listing/a.csv', 'memory://listing/b.csv'] as $uri) {
            $stream = $filesystem->writeTo(path($uri));
            $stream->append("id\n1\n");
            $stream->close();
        }

        $statuses = iterator_to_array(
            (new FileListing($filesystem))->list(path('memory://listing/*.csv'), new KeepAll()),
            false,
        );

        static::assertCount(2, $statuses);
        static::assertContainsOnlyInstancesOf(FileStatus::class, $statuses);
    }

    public function test_listing_opens_no_file_descriptors(): void
    {
        if (!$this->isUnix()) {
            static::markTestSkipped('Open descriptors are counted through /dev/fd, which is Unix only.');
        }

        for ($i = 1; $i <= 4; $i++) {
            $this->givenFileExists(__DIR__ . '/var/descriptors/file-' . $i . '.csv', "id\n" . $i . "\n");
        }

        $listing = new FileListing(new NativeLocalFilesystem());

        $before = count(type_list(type_string())->assert(scandir('/dev/fd')));
        $statuses = iterator_to_array($listing->list(path(__DIR__ . '/var/descriptors/*.csv'), new KeepAll()), false);
        $after = count(type_list(type_string())->assert(scandir('/dev/fd')));

        static::assertCount(4, $statuses);
        static::assertSame($before, $after);
    }

    public function test_placeholder_partitions_are_attached(): void
    {
        $filesystem = memory_filesystem();

        // the placeholder names a partition hive parsing cannot recover - the filename itself carries the value
        foreach (['123456-PL', '789012-DE'] as $order) {
            $stream = $filesystem->writeTo(path('memory://orders/' . $order . '.csv'));
            $stream->append("id\n1\n");
            $stream->close();
        }

        $statuses = iterator_to_array(
            (new FileListing($filesystem))->list(path('memory://orders/{order-name}.csv'), new KeepAll()),
            false,
        );

        static::assertCount(2, $statuses);
        static::assertSame('123456-PL', $statuses[0]->path->partitions()->get('order-name')->value);
        static::assertSame('789012-DE', $statuses[1]->path->partitions()->get('order-name')->value);
    }
}
