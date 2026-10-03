<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\ListColumn;
use Flow\ETL\Column\Php\ListColumnBuilder;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Physical\IdentityPhysical;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class ListColumnBuilderTest extends TestCase
{
    public function test_builds_offsets_and_nulls(): void
    {
        $builder = new ListColumnBuilder(
            type_list(type_integer()),
            new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
        );
        $builder->appendPhysical([5 => 1, 9 => 2]);
        $builder->appendPhysicals([null, [], [3]]);

        static::assertSame(4, $builder->count());

        $column = $builder->finish();

        static::assertInstanceOf(ListColumn::class, $column);
        static::assertSame([0, 2, 2, 2, 3], $column->offsets);
        static::assertSame([1 => true], $column->nulls);
        static::assertSame([[1, 2], null, [], [3]], $column->physicals());
    }

    public function test_a_bulk_append_builds_the_column_row_by_row_appends_build(): void
    {
        $physicals = [[[1, 2], []], null, [], [null, [3]], [5 => [4]]];
        $builder = static fn(): ListColumnBuilder => new ListColumnBuilder(
            type_list(type_list(type_integer())),
            new ListColumnBuilder(
                type_list(type_integer()),
                new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
            ),
        );
        $bulk = $builder();
        $bulk->appendPhysicals($physicals);
        $rowByRow = $builder();

        foreach ($physicals as $physical) {
            $rowByRow->appendPhysical($physical);
        }

        static::assertEquals($rowByRow->finish(), $bulk->finish());
        static::assertSame([[[1, 2], []], null, [], [null, [3]], [[4]]], $bulk->finish()->physicals());
    }
}
