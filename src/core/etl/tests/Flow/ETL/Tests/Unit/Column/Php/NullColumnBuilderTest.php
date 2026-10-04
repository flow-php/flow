<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Column\Php\NullColumnBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_null;

final class NullColumnBuilderTest extends TestCase
{
    public function test_counts_rows_into_a_null_constant(): void
    {
        $builder = new NullColumnBuilder(type_null());
        $builder->appendPhysical(null);
        $builder->appendPhysicals([null, null]);

        static::assertSame(3, $builder->count());

        $column = $builder->finish();

        static::assertInstanceOf(ConstantColumn::class, $column);
        static::assertSame(3, $column->nullCount());
    }
}
