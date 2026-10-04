<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\MapColumn;
use Flow\ETL\Column\Php\MapColumnBuilder;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Physical\IdentityPhysical;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class MapColumnBuilderTest extends TestCase
{
    public function test_builds_offsets_keys_values_and_nulls(): void
    {
        $builder = new MapColumnBuilder(
            type_map(type_string(), type_integer()),
            new ScalarColumnBuilder(type_string(), new IdentityPhysical()),
            new ScalarColumnBuilder(type_integer(), new IdentityPhysical()),
        );
        $builder->appendPhysical(['a' => 1, 'b' => 2]);
        $builder->appendPhysicals([null, ['c' => 3]]);

        static::assertSame(3, $builder->count());

        $column = $builder->finish();

        static::assertInstanceOf(MapColumn::class, $column);
        static::assertSame([0, 2, 2, 3], $column->offsets);
        static::assertSame([1 => true], $column->nulls);
        static::assertSame(['a', 'b', 'c'], $column->keys->physicals());
        static::assertSame([1, 2, 3], $column->values->physicals());
    }
}
