<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Tests\Comparator\RowComparator;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowComparatorTest extends TestCase
{
    public function test_accepts_only_two_rows(): void
    {
        $comparator = new RowComparator();
        $row = array_to_row(['id' => 1], schema(int_schema('id')));

        static::assertTrue($comparator->accepts($row, $row));
        static::assertFalse($comparator->accepts($row, rows(schema(int_schema('id')), $row)));
        static::assertFalse($comparator->accepts($row, ['id' => 1]));
    }

    public function test_rows_of_different_batches_with_equal_values_pass(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(
            array_to_rows([['id' => 1]], schema(int_schema('id')))->first(),
            array_to_rows([['id' => 0], ['id' => 1]], schema(int_schema('id')))->last(),
        );

        $this->addToAssertionCount(1);
    }

    public function test_the_schema_is_not_compared(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(array_to_row([
            'phase' => null,
        ], schema(str_schema('phase', nullable: true))), array_to_rows([['phase' => null]], schema(int_schema('phase', nullable: true)))->first());

        $this->addToAssertionCount(1);
    }

    public function test_a_name_order_difference_fails(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $this->expectException(ComparisonFailure::class);
        $this->expectExceptionMessage('Failed asserting that two Row objects have the same names.');

        $comparator->assertEquals(
            array_to_row(['a' => 1, 'b' => 2], schema(int_schema('a'), int_schema('b'))),
            array_to_row(['b' => 2, 'a' => 1], schema(int_schema('b'), int_schema('a'))),
        );
    }

    public function test_a_value_difference_fails(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        try {
            $comparator->assertEquals(
                array_to_row(['id' => 1], schema(int_schema('id'))),
                array_to_row(['id' => 2], schema(int_schema('id'))),
            );
        } catch (ComparisonFailure $failure) {
            static::assertSame('Failed asserting that two Row objects are equal.', $failure->getMessage());
            static::assertStringContainsString('1', $failure->getExpectedAsString());
            static::assertStringContainsString('2', $failure->getActualAsString());

            return;
        }

        static::fail('two Row objects with different values compared equal');
    }
}
