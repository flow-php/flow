<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\{and_, col, eq, literal, schema_view, select, star, table};

use Flow\PostgreSql\Schema\Diff\ViewDiff;
use PHPUnit\Framework\TestCase;

final class ViewDiffTest extends TestCase
{
    public function test_generates_create_or_replace_view() : void
    {
        $diff = new ViewDiff(
            schema_view('active_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()),
            schema_view('active_users', select(star())->from(table('users'))->where(and_(eq(col('active'), literal(true)), eq(col('verified'), literal(true))))->toSql()),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE active = true AND verified = true', $sqls[0]->toSql());
    }

    public function test_reversed_view_definition_change() : void
    {
        $diff = new ViewDiff(
            schema_view('active_users', select(star())->from(table('users'))->where(and_(eq(col('active'), literal(true)), eq(col('verified'), literal(true))))->toSql()),
            schema_view('active_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()),
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE active = true', $sqls[0]->toSql());
    }
}
