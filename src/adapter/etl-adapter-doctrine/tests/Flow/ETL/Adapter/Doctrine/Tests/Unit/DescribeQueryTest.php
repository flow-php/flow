<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\DescribeQuery;
use Flow\ETL\Tests\FlowTestCase;

final class DescribeQueryTest extends FlowTestCase
{
    public function test_a_plain_query_is_wrapped(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM t\n) flow_describe WHERE 1=0",
            (new DescribeQuery())->of('SELECT id FROM t'),
        );
    }

    public function test_a_trailing_line_comment_is_closed_by_the_newline(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM t -- why\n) flow_describe WHERE 1=0",
            (new DescribeQuery())->of('SELECT id FROM t -- why'),
        );
    }

    public function test_a_trailing_semicolon_is_stripped(): void
    {
        static::assertSame(
            "SELECT * FROM (\nSELECT id FROM t\n) flow_describe WHERE 1=0",
            (new DescribeQuery())->of("SELECT id FROM t;  \n"),
        );
    }
}
