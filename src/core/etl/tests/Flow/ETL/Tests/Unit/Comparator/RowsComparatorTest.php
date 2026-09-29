<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Comparator\RowsComparator;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsComparatorTest extends TestCase
{
    public function test_accepts_only_two_rows(): void
    {
        $comparator = new RowsComparator();
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        static::assertTrue($comparator->accepts($rows, $rows));
        static::assertFalse($comparator->accepts($rows, ['id' => 1]));
        static::assertFalse($comparator->accepts([], $rows));
    }

    public function test_equal_rows_of_two_batches_pass(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        $comparator->assertEquals(
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
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
            array_to_rows([['id' => 7], ['id' => 7]], schema(int_schema('id'))),
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
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
            array_to_rows([['id' => 1]], schema(int_schema('id', nullable: true))),
        );
    }

    public function test_a_value_difference_fails(): void
    {
        $comparator = new RowsComparator();
        $comparator->setFactory(Factory::getInstance());

        try {
            $comparator->assertEquals(
                array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))),
                array_to_rows([['id' => 1, 'name' => 'b']], schema(int_schema('id'), str_schema('name'))),
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
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        static::assertNotEquals(
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );
    }
}
