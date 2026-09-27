<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Comparator\RowsComparator;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsComparatorTest extends TestCase
{
    public function test_accepts_only_two_rows(): void
    {
        $comparator = new RowsComparator();
        $rows = rows(schema(int_schema('id')), row(['id' => 1]));

        static::assertTrue($comparator->accepts($rows, $rows));
        static::assertFalse($comparator->accepts($rows, row(['id' => 1])));
        static::assertFalse($comparator->accepts([], $rows));
    }

    public function test_equal_rows_of_two_batches_pass(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );

        $this->addToAssertionCount(1);
    }

    public function test_equal_across_column_classes(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(
            Rows::fromColumns(
                schema(int_schema('id')),
                ['id' => (new PhpBackend())->constant(int_schema('id'), 7, 2)],
                2,
            ),
            rows(schema(int_schema('id')), row(['id' => 7]), row(['id' => 7])),
        );

        $this->addToAssertionCount(1);
    }

    public function test_a_schema_difference_fails(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        $this->expectException(ComparisonFailure::class);
        $this->expectExceptionMessage('Failed asserting that two Rows have the same schema.');

        $comparator->assertEquals(
            rows(schema(int_schema('id')), row(['id' => 1])),
            rows(schema(int_schema('id', nullable: true)), row(['id' => 1])),
        );
    }

    public function test_a_value_difference_fails(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        try {
            $comparator->assertEquals(
                rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])),
                rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'b'])),
            );
        } catch (ComparisonFailure $failure) {
            static::assertSame('Failed asserting that two Rows are equal.', $failure->getMessage());
            static::assertStringContainsString("'a'", $failure->getExpectedAsString());
            static::assertStringContainsString("'b'", $failure->getActualAsString());

            return;
        }

        static::fail('two Rows with different values compared equal');
    }

    public function test_assert_equals_uses_the_registered_comparator(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id')), row(['id' => 1])),
            rows(schema(int_schema('id')), row(['id' => 1])),
        );
        static::assertNotEquals(
            rows(schema(int_schema('id')), row(['id' => 1])),
            rows(schema(int_schema('id')), row(['id' => 2])),
        );
    }
}
