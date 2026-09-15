<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\DescribeQuery;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\PostgreSqlErrorCategory;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class DescribeQueryTest extends TestCase
{
    public function test_an_error_inside_the_query_is_located_in_the_callers_sql(): void
    {
        // PostgreSQL reported 32 for this query wrapped in the probe; "missing" is at 16 in the caller's SQL.
        static::assertSame(
            16,
            (new DescribeQuery())->errorIn('SELECT id FROM missing', PostgreSqlError::fromDiagnostics(
                '42P01',
                'relation "missing" does not exist',
                position: 32,
            ))->position,
        );
    }

    public function test_a_stripped_trailing_semicolon_is_outside_the_callers_sql(): void
    {
        static::assertNull((new DescribeQuery())->errorIn('SELECT id FROM missing;', PostgreSqlError::fromDiagnostics(
            '42601',
            'syntax error',
            position: 40,
        ))->position);
    }

    #[TestWith([17, 1])]
    #[TestWith([39, 23])]
    public function test_an_error_at_the_edges_of_the_callers_sql_keeps_its_position(int $wrapped, int $position): void
    {
        static::assertSame(
            $position,
            (new DescribeQuery())->errorIn('SELECT id FROM missing', PostgreSqlError::fromDiagnostics(
                '42601',
                'syntax error',
                position: $wrapped,
            ))->position,
        );
    }

    public function test_an_error_without_diagnostics_keeps_its_category(): void
    {
        static::assertSame(
            PostgreSqlErrorCategory::UNKNOWN,
            (new DescribeQuery())->errorIn(
                'SELECT id FROM orders',
                PostgreSqlError::unknown('connection lost'),
            )->category,
        );
    }

    #[TestWith([5])]
    #[TestWith([16])]
    #[TestWith([40])]
    #[TestWith([200])]
    public function test_an_error_outside_the_callers_sql_has_no_position(int $position): void
    {
        static::assertNull((new DescribeQuery())->errorIn('SELECT id FROM missing', PostgreSqlError::fromDiagnostics(
            '42601',
            'syntax error',
            position: $position,
        ))->position);
    }

    public function test_an_error_without_a_position_keeps_none(): void
    {
        static::assertNull((new DescribeQuery())->errorIn('SELECT id FROM orders', PostgreSqlError::fromDiagnostics(
            '42501',
            'denied',
        ))->position);
    }

    public function test_every_diagnostic_but_the_position_is_kept(): void
    {
        $error = (new DescribeQuery())->errorIn('SELECT id FROM orders', PostgreSqlError::fromDiagnostics(
            '42703',
            'column',
            'detail',
            'hint',
            'public',
            'orders',
            'id',
            'pk',
            20,
        ));

        static::assertSame('42703', $error->sqlState);
        static::assertSame('column', $error->message);
        static::assertSame('detail', $error->detail);
        static::assertSame('hint', $error->hint);
        static::assertSame('public', $error->schema);
        static::assertSame('orders', $error->table);
        static::assertSame('id', $error->column);
        static::assertSame('pk', $error->constraint);
    }

    public function test_a_plain_query_is_wrapped(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM orders\n) flow_describe LIMIT 0",
            (new DescribeQuery())->of('SELECT id FROM orders'),
        );
    }

    public function test_a_sql_object_is_rendered_through_to_sql(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id, label FROM orders\n) flow_describe LIMIT 0",
            (new DescribeQuery())->of(select(col('id'), col('label'))->from(table('orders'))),
        );
    }

    public function test_a_trailing_line_comment_is_closed_by_the_newline(): void
    {
        // Without the closing newline PostgreSQL reads ") flow_describe LIMIT 0" as part of the
        // comment and reports "syntax error at end of input".
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM orders -- trailing\n) flow_describe LIMIT 0",
            (new DescribeQuery())->of('SELECT id FROM orders -- trailing'),
        );
    }

    public function test_a_trailing_semicolon_is_stripped(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM orders\n) flow_describe LIMIT 0",
            (new DescribeQuery())->of("SELECT id FROM orders;  \n"),
        );
    }
}
