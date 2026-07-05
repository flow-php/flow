<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path\Filter;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Path\Filter\PlaceholderPartitions;
use Flow\Filesystem\Tests\Double\PartitionsCollectingFilter;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;

final class PlaceholderPartitionsTest extends TestCase
{
    public function test_attaches_placeholder_partitions_to_file_status_before_delegating(): void
    {
        $collectingFilter = new PartitionsCollectingFilter();
        $filter = new PlaceholderPartitions(path('/output/order-year=*/{order-name}.csv'), $collectingFilter);

        static::assertTrue($filter->accept(
            new FileStatus(path('/output/order-year=2024/123456-PL.csv'), isFile: true),
        ));
        static::assertCount(1, $collectingFilter->partitionsList);
        static::assertSame('2024', $collectingFilter->partitionsList[0]->get('order-year')->value);
        static::assertSame('123456-PL', $collectingFilter->partitionsList[0]->get('order-name')->value);
    }

    public function test_delegates_original_file_status_when_no_partitions_extracted(): void
    {
        $collectingFilter = new PartitionsCollectingFilter();
        $filter = new PlaceholderPartitions(path('/output/{order-name}.csv'), $collectingFilter);

        static::assertTrue($filter->accept(new FileStatus(path('/other/123456-PL.csv'), isFile: true)));
        static::assertCount(1, $collectingFilter->partitionsList);
        static::assertCount(0, $collectingFilter->partitionsList[0]);
    }

    public function test_delegates_to_decorated_filter_decision(): void
    {
        $filter = new PlaceholderPartitions(path('/output/{order-name}.csv'), new class implements Filter {
            public function accept(FileStatus $status): bool
            {
                return false;
            }
        });

        static::assertFalse($filter->accept(new FileStatus(path('/output/123456-PL.csv'), isFile: true)));

        static::assertTrue((new PlaceholderPartitions(path('/output/{order-name}.csv'), new KeepAll()))->accept(
            new FileStatus(path('/output/123456-PL.csv'), isFile: true),
        ));
    }
}
