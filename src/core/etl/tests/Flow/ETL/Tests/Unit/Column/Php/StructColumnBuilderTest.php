<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\ListColumnBuilder;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Php\StructColumn;
use Flow\ETL\Column\Php\StructColumnBuilder;
use Flow\ETL\Column\Physical\IdentityPhysical;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_structure;

final class StructColumnBuilderTest extends TestCase
{
    public function test_a_null_parent_sets_every_child_bit_to_zero(): void
    {
        $builder = new StructColumnBuilder(type_structure(['a' => type_integer(), 'b' => type_integer()]), [
            'a' => new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
            'b' => new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
        ]);
        $builder->appendPhysical(['b' => 2, 'a' => 1]);
        $builder->appendPhysicals([null, ['a' => 3]]);

        static::assertSame(3, $builder->count());

        $column = $builder->finish();

        static::assertInstanceOf(StructColumn::class, $column);
        static::assertSame([1 => true], $column->nulls);
        static::assertSame([1, null, 3], $column->children['a']->physicals());
        static::assertSame([2, null, null], $column->children['b']->physicals());
    }

    public function test_a_bulk_append_builds_the_column_row_by_row_appends_build(): void
    {
        $physicals = [['a' => 1, 'b' => [1, 2]], null, ['b' => []], ['a' => null, 'b' => null], ['b' => [3], 'a' => 4]];
        $builder = static fn(): StructColumnBuilder => new StructColumnBuilder(
            type_structure(['a' => type_integer(), 'b' => type_list(type_integer())]),
            [
                'a' => new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
                'b' => new ListColumnBuilder(
                    type_list(type_integer()),
                    new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
                ),
            ],
        );
        $bulk = $builder();
        $bulk->appendPhysicals($physicals);
        $rowByRow = $builder();

        foreach ($physicals as $physical) {
            $rowByRow->appendPhysical($physical);
        }

        static::assertEquals($rowByRow->finish(), $bulk->finish());
        static::assertSame(
            [
                ['a' => 1, 'b' => [1, 2]],
                null,
                ['a' => null, 'b' => []],
                ['a' => null, 'b' => null],
                ['a' => 4, 'b' => [3]],
            ],
            $bulk->finish()->physicals(),
        );
    }
}
