<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Comparator\ColumnComparator;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;

final class ColumnComparatorTest extends TestCase
{
    public function test_accepts_only_two_columns(): void
    {
        $column = ColumnMother::of(int_schema('a'), [1]);

        static::assertTrue((new ColumnComparator())->accepts($column, $column));
        static::assertFalse((new ColumnComparator())->accepts($column, [1]));
        static::assertFalse((new ColumnComparator())->accepts([1], $column));
    }

    public function test_columns_of_both_backends_with_equal_values_pass(): void
    {
        $comparator = new ColumnComparator();
        $comparator->setFactory(Factory::getInstance());
        $default = (new AdaptiveBackend())->builder(int_schema('a', nullable: true));
        $default->appendMany([1, null]);

        $comparator->assertEquals(ColumnMother::of(int_schema('a', nullable: true), [1, null]), $default->finish());

        $this->addToAssertionCount(1);
    }

    public function test_different_values_fail(): void
    {
        $comparator = new ColumnComparator();
        $comparator->setFactory(Factory::getInstance());

        $this->expectException(ComparisonFailure::class);
        $this->expectExceptionMessage('Failed asserting that two columns are equal.');

        $comparator->assertEquals(
            (new PhpBackend())->constant(int_schema('a'), 1, 2),
            ColumnMother::of(int_schema('a'), [1, 3]),
        );
    }

    public function test_different_types_fail(): void
    {
        $comparator = new ColumnComparator();
        $comparator->setFactory(Factory::getInstance());

        $this->expectException(ComparisonFailure::class);
        $this->expectExceptionMessage('Failed asserting that two columns have the same type.');

        $comparator->assertEquals(ColumnMother::of(int_schema('a'), [1]), ColumnMother::of(str_schema('a'), ['1']));
    }
}
