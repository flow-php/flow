<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Query;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_query;

final class QueryTest extends TestCase
{
    public function test_a_new_query_reads_every_row_of_every_column(): void
    {
        $query = parquet_query();

        static::assertSame([], $query->columns);
        static::assertNull($query->filter);
        static::assertSame(0, $query->offset);
        static::assertNull($query->limit);
    }

    public function test_each_builder_step_returns_a_new_query(): void
    {
        $filter = parquet_column('id')->greaterThan(1);
        $base = parquet_query();

        $query = $base->select('id', 'name')->where($filter)->offset(5)->limit(10);

        static::assertEquals(new Query(['id', 'name'], $filter, 5, 10), $query);
        static::assertEquals(new Query(), $base);
    }

    public function test_a_second_filter_narrows_the_first(): void
    {
        $first = parquet_column('id')->greaterThan(1);
        $second = parquet_column('id')->lessThan(9);

        static::assertEquals(new All($first, $second), parquet_query()->where($first)->where($second)->filter);
    }

    public function test_a_third_filter_extends_the_all_instead_of_nesting_one(): void
    {
        $first = parquet_column('id')->greaterThan(1);
        $second = parquet_column('id')->lessThan(9);
        $third = parquet_column('id')->notEquals(5);

        static::assertEquals(
            new All($first, $second, $third),
            parquet_query()->where($first)->where($second)->where($third)->filter,
        );
    }

    public function test_select_without_columns_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A query selects at least one column');

        parquet_query()->select();
    }

    public function test_an_empty_column_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A query column name cannot be empty');

        parquet_query()->select('id', '');
    }

    public function test_a_column_selected_twice_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A query selects each column once');

        parquet_query()->select('id', 'id');
    }

    #[TestWith([-1, 'Query offset must be 0 or more, got -1'])]
    public function test_a_negative_offset_is_refused(int $offset, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        parquet_query()->offset($offset);
    }

    #[TestWith([-1, 'Query limit must be 0 or more, got -1'])]
    public function test_a_negative_limit_is_refused(int $limit, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        parquet_query()->limit($limit);
    }

    public function test_a_limit_of_zero_is_allowed(): void
    {
        static::assertSame(0, parquet_query()->limit(0)->limit);
    }
}
