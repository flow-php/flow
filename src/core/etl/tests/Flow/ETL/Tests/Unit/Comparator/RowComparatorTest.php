<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Tests\Comparator\RowComparator;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class RowComparatorTest extends TestCase
{
    public function test_accepts_only_two_rows(): void
    {
        $comparator = new RowComparator();
        $row = row(['id' => 1]);

        static::assertTrue($comparator->accepts($row, $row));
        static::assertFalse($comparator->accepts($row, rows(schema(int_schema('id')), $row)));
        static::assertFalse($comparator->accepts($row, ['id' => 1]));
    }

    public function test_rows_of_different_batches_with_equal_values_pass(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(
            rows(schema(int_schema('id')), row(['id' => 1]))->first(),
            rows(schema(int_schema('id')), row(['id' => 0]), row(['id' => 1]))->last(),
        );

        $this->addToAssertionCount(1);
    }

    public function test_the_schema_is_not_compared(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(row([
            'phase' => null,
        ]), rows(schema(int_schema('phase', nullable: true)), row(['phase' => null]))->first());

        $this->addToAssertionCount(1);
    }

    public function test_a_name_order_difference_fails(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        $this->expectException(ComparisonFailure::class);
        $this->expectExceptionMessage('Failed asserting that two Row objects have the same names.');

        $comparator->assertEquals(row(['a' => 1, 'b' => 2]), row(['b' => 2, 'a' => 1]));
    }

    public function test_a_value_difference_fails(): void
    {
        $comparator = new RowComparator();
        $comparator->setFactory(Factory::getInstance());

        try {
            $comparator->assertEquals(row(['id' => 1]), row(['id' => 2]));
        } catch (ComparisonFailure $failure) {
            static::assertSame('Failed asserting that two Row objects are equal.', $failure->getMessage());
            static::assertStringContainsString('1', $failure->getExpectedAsString());
            static::assertStringContainsString('2', $failure->getActualAsString());

            return;
        }

        static::fail('two Row objects with different values compared equal');
    }
}
