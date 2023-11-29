<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Partition;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Partition;
use Flow\ETL\Partition\ScalarFunctionFilter;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PHPUnit\Framework\TestCase;

final class ScalarFunctionFilterTest extends TestCase
{
    public function test_filtering() : void
    {
        $filter = new ScalarFunctionFilter(
            ref('foo')->greaterThan(lit(10)),
            new NativeEntryFactory()
        );

        $this->assertTrue($filter->keep(new Partition('foo', '100')));
        $this->assertFalse($filter->keep(new Partition('foo', '5')));
    }

    public function test_filtering_when_partition_is_not_covered_by_any_filter() : void
    {
        $filter = new ScalarFunctionFilter(
            ref('foo')->greaterThan(lit(10)),
            new NativeEntryFactory()
        );

        $this->assertFalse($filter->keep(new Partition('bar', '100')));
    }

    public function test_filtering_with_multiple_partitions_and_condition() : void
    {
        $filter = new ScalarFunctionFilter(
            all(
                ref('foo')->greaterThanEqual(lit(100)),
                ref('bar')->greaterThanEqual(lit(100))
            ),
            new NativeEntryFactory()
        );

        $this->assertTrue($filter->keep(new Partition('foo', '100'), new Partition('bar', '100')));
        $this->assertFalse($filter->keep(new Partition('foo', '100'), new Partition('bar', '5')));
        $this->assertFalse($filter->keep(new Partition('foo', '5'), new Partition('bar', '100')));
        $this->assertFalse($filter->keep(new Partition('foo', '5'), new Partition('bar', '5')));
    }

    public function test_filtering_with_multiple_partitions_or_condition() : void
    {
        $filter = new ScalarFunctionFilter(
            any(
                ref('foo')->greaterThanEqual(lit(100)),
                ref('bar')->greaterThanEqual(lit(100))
            ),
            new NativeEntryFactory()
        );

        $this->assertTrue($filter->keep(new Partition('foo', '100'), new Partition('bar', '100')));
        $this->assertTrue($filter->keep(new Partition('foo', '100'), new Partition('bar', '5')));
        $this->assertTrue($filter->keep(new Partition('foo', '5'), new Partition('bar', '100')));
        $this->assertFalse($filter->keep(new Partition('foo', '5'), new Partition('bar', '5')));
    }
}
