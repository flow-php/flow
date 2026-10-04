<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Physical\IdentityPhysical;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;

final class ScalarColumnBuilderTest extends TestCase
{
    public function test_collects_physicals_and_counts_nulls(): void
    {
        $builder = new ScalarColumnBuilder(type_optional(type_integer()), new IdentityPhysical());
        $builder->appendPhysical(1);
        $builder->appendPhysical(null);
        $builder->appendPhysicals([null, 4]);

        static::assertSame(4, $builder->count());

        $column = $builder->finish();

        static::assertSame([1, null, null, 4], $column->physicals());
        static::assertSame(2, $column->nullCount());
    }

    public function test_takes_the_null_count_the_caller_already_counted(): void
    {
        $builder = new ScalarColumnBuilder(type_optional(type_integer()), new IdentityPhysical());
        $builder->appendPhysicals([1, null], 1);
        $builder->appendPhysicals([null, null, 5], 2);

        $column = $builder->finish();

        static::assertSame([1, null, null, null, 5], $column->physicals());
        static::assertSame(3, $column->nullCount());
    }

    public function test_a_bulk_append_counts_only_nulls(): void
    {
        $builder = new ScalarColumnBuilder(type_optional(type_integer()), new IdentityPhysical());
        $builder->appendPhysicals([0, '', false, null, '0']);

        $column = $builder->finish();

        static::assertSame([0, '', false, null, '0'], $column->physicals());
        static::assertSame(1, $column->nullCount());
    }
}
