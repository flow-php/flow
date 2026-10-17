<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use DateTime;
use DateTimeImmutable;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Filter\StartsWith;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class ColumnRefTest extends TestCase
{
    public function test_each_comparison_method_builds_its_operator(): void
    {
        $column = parquet_column('user.age');

        static::assertEquals(new Comparison($column, Operator::Equal, 1), $column->equals(1));
        static::assertEquals(new Comparison($column, Operator::LessThan, 1), $column->lessThan(1));
        static::assertEquals(new Comparison($column, Operator::LessThanOrEqual, 1), $column->lessThanOrEqual(1));
        static::assertEquals(new Comparison($column, Operator::GreaterThan, 1), $column->greaterThan(1));
        static::assertEquals(new Comparison($column, Operator::GreaterThanOrEqual, 1), $column->greaterThanOrEqual(1));
    }

    public function test_the_negative_methods_negate_their_positive_node(): void
    {
        $column = parquet_column('id');

        static::assertEquals(new Not($column->equals(1)), $column->notEquals(1));
        static::assertEquals(new Not(new In($column, 1, 2)), $column->notIn([1, 2]));
        static::assertEquals(new Not(new IsNull($column)), $column->isNotNull());
    }

    public function test_set_null_and_prefix_methods_build_their_leaf(): void
    {
        $column = parquet_column('name');

        static::assertSame([1, 'a'], $column->in(['x' => 1, 'y' => 'a'])->values);
        static::assertSame([], $column->in([])->values);
        static::assertEquals(new IsNull($column), $column->isNull());
        static::assertEquals(new StartsWith($column, 'ab'), $column->startsWith('ab'));
    }

    public function test_a_date_time_literal_is_stored_immutable(): void
    {
        $value = parquet_column('ts')->equals(new DateTime('2024-01-02 03:04:05.678901 UTC'))->value;

        static::assertInstanceOf(DateTimeImmutable::class, $value);
        static::assertSame('2024-01-02 03:04:05.678901', $value->format('Y-m-d H:i:s.u'));
    }

    public function test_an_empty_path_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A column path cannot be empty');

        parquet_column('');
    }
}
