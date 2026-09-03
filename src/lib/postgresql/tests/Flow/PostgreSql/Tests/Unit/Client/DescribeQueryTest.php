<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\DescribeQuery;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class DescribeQueryTest extends TestCase
{
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
