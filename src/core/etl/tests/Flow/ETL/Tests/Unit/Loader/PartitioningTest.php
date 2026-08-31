<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\partition_by;

final class PartitioningTest extends FlowTestCase
{
    public function test_none_partitions_by_nothing(): void
    {
        static::assertCount(0, Partitioning::none()->by);
        static::assertFalse(Partitioning::none()->writeColumns);
    }

    public function test_write_columns_on_none_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('writeColumns requires at least one partition column');

        Partitioning::none()->writeColumns();
    }

    public function test_the_dsl_delegates_without_normalising(): void
    {
        static::assertEquals(Partitioning::by('region'), partition_by('region'));
    }

    public function test_write_columns_is_off_by_default_and_immutable(): void
    {
        $partitioning = Partitioning::by('region');

        static::assertFalse($partitioning->writeColumns);
        static::assertTrue($partitioning->writeColumns()->writeColumns);
        static::assertFalse($partitioning->writeColumns);
    }
}
