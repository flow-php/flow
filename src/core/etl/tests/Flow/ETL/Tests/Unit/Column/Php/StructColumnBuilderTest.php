<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\IdentityPhysical;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Php\StructColumn;
use Flow\ETL\Column\Php\StructColumnBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
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
        $builder->appendPhysicalMany([null, ['a' => 3]]);

        static::assertSame(3, $builder->count());

        $column = $builder->finish();

        static::assertInstanceOf(StructColumn::class, $column);
        static::assertSame([1 => true], $column->nulls);
        static::assertSame([1, null, 3], $column->children['a']->physicals());
        static::assertSame([2, null, null], $column->children['b']->physicals());
    }
}
