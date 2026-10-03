<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\ScalarColumn;
use Flow\ETL\Column\Physical\TimeZonePhysical;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\Types\DSL\type_time_zone;

final class ScalarColumnTest extends TestCase
{
    public function test_reads_physical_and_logical_cells(): void
    {
        $column = ColumnMother::of(time_zone_schema('a', nullable: true), ['UTC', null]);

        static::assertInstanceOf(ScalarColumn::class, $column);
        static::assertSame(2, $column->count());
        static::assertSame('UTC', $column->at(0));
        static::assertNull($column->at(1));
        static::assertSame(['UTC', null], $column->physicals());
        static::assertEquals(new DateTimeZone('UTC'), $column->value(0));
        static::assertNull($column->value(1));
        static::assertTrue($column->isNull(1));
        static::assertFalse($column->isNull(0));
        static::assertSame(1, $column->nullCount());
        static::assertEquals(type_time_zone(), $column->type());
    }

    public function test_slice_and_take_recount_nulls(): void
    {
        $column = ColumnMother::of(int_schema('a', nullable: true), [1, null, 3, null]);

        static::assertSame([null, 3], $column->slice(1, 2)->physicals());
        static::assertSame(1, $column->slice(1, 2)->nullCount());
        static::assertSame(0, $column->slice(2, 1)->nullCount());
        static::assertSame([3, 1, 3], $column->take([2, 0, 2])->physicals());
        static::assertSame(0, $column->take([2, 0, 2])->nullCount());
        static::assertSame(2, $column->take([1, 3, 0])->nullCount());
    }

    public function test_a_column_without_nulls_slices_without_counting(): void
    {
        $column = new ScalarColumn(type_time_zone(), new TimeZonePhysical(), ['UTC', 'UTC'], 0);

        static::assertSame(0, $column->slice(0, 1)->nullCount());
        static::assertSame(0, $column->take([1])->nullCount());
    }
}
