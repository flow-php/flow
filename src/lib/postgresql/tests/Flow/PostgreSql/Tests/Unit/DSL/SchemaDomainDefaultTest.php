<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use function Flow\PostgreSql\DSL\{column_type_integer, column_type_text, column_type_timestamptz, func, schema_domain};

use PHPUnit\Framework\TestCase;

final class SchemaDomainDefaultTest extends TestCase
{
    public function test_func_call_expression_is_stored_as_deparsed_sql() : void
    {
        $domain = schema_domain('ts', column_type_timestamptz(), default: func('now'));

        self::assertSame('now()', $domain->default);
    }

    public function test_int_default_is_stored_as_literal() : void
    {
        $domain = schema_domain('positive_int', column_type_integer(), default: 0);

        self::assertSame('0', $domain->default);
    }

    public function test_null_default_is_stored_as_null() : void
    {
        self::assertNull(schema_domain('email', column_type_text())->default);
    }

    public function test_plain_string_default_is_stored_as_pg_quoted_literal() : void
    {
        $domain = schema_domain('email', column_type_text(), default: 'unknown');

        self::assertSame("'unknown'", $domain->default);
    }

    public function test_round_trip_through_to_sql() : void
    {
        $domain = schema_domain('email', column_type_text(), default: 'unknown');
        $sql = $domain->toSql()->toSql();

        self::assertStringContainsString("DEFAULT 'unknown'", $sql);
    }
}
