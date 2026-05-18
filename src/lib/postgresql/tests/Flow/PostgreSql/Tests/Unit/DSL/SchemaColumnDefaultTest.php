<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_column;
use function Flow\PostgreSql\DSL\schema_column_boolean;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_real;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_column_timestamp_tz;
use function Flow\PostgreSql\DSL\schema_column_uuid;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_table;
use function implode;

final class SchemaColumnDefaultTest extends TestCase
{
    public function test_bool_false_default_is_stored_as_literal(): void
    {
        $column = schema_column_boolean('active', default: false);

        static::assertSame('false', $column->default);
    }

    public function test_bool_true_default_is_stored_as_literal(): void
    {
        $column = schema_column_boolean('active', default: true);

        static::assertSame('true', $column->default);
    }

    public function test_float_default_is_stored_as_literal(): void
    {
        $column = schema_column_real('price', default: 3.14);

        static::assertSame('3.14', $column->default);
    }

    public function test_func_call_expression_is_stored_as_deparsed_sql(): void
    {
        $column = schema_column_timestamp_tz('created_at', default: func('now'));

        static::assertSame('now()', $column->default);
    }

    public function test_func_call_uuid_default(): void
    {
        $column = schema_column_uuid('external_id', default: func('gen_random_uuid'));

        static::assertSame('gen_random_uuid()', $column->default);
    }

    public function test_int_default_is_stored_as_literal(): void
    {
        static::assertSame('0', schema_column_integer('age', default: 0)->default);
        static::assertSame('-42', schema_column_integer('delta', default: -42)->default);
    }

    public function test_literal_expression_matches_plain_scalar_equivalent(): void
    {
        static::assertSame(
            schema_column('status', column_type_varchar(50), default: 'active')->default,
            schema_column('status', column_type_varchar(50), default: literal('active'))->default,
        );
        static::assertSame(
            schema_column_integer('age', default: 0)->default,
            schema_column_integer('age', default: literal(0))->default,
        );
    }

    public function test_negative_float_default_is_stored_as_literal(): void
    {
        $column = schema_column_real('delta', default: -2.5);

        static::assertSame('-2.5', $column->default);
    }

    public function test_null_default_is_stored_as_null(): void
    {
        static::assertNull(schema_column('name', column_type_varchar(50))->default);
        static::assertNull(schema_column('name', column_type_varchar(50), default: null)->default);
    }

    public function test_plain_string_default_is_stored_as_pg_quoted_literal(): void
    {
        $column = schema_column('status', column_type_varchar(50), default: 'active');

        static::assertSame("'active'", $column->default);
    }

    public function test_round_trip_through_to_sql(): void
    {
        $table = schema_table('events', [
            schema_column_varchar('status', 50, nullable: false, default: 'active'),
            schema_column_timestamp_tz('created_at', nullable: false, default: func('now')),
            schema_column_integer('priority', nullable: false, default: 0),
        ]);

        $statements = $table->toSql();
        $sql = implode("\n", array_map(static fn($stmt) => $stmt->toSql(), $statements));

        static::assertStringContainsString("DEFAULT 'active'", $sql);
        static::assertStringContainsString('DEFAULT now()', $sql);
        static::assertStringContainsString('DEFAULT 0', $sql);
    }

    public function test_schema_column_text_accepts_typed_string_default(): void
    {
        $column = schema_column_text('label', default: 'unknown');

        static::assertSame("'unknown'", $column->default);
    }

    public function test_schema_column_varchar_accepts_typed_default(): void
    {
        $column = schema_column_varchar('status', 50, default: 'pending');

        static::assertSame("'pending'", $column->default);
    }

    public function test_string_with_backslash_is_stored_verbatim_inside_literal(): void
    {
        $column = schema_column('path', column_type_varchar(50), default: 'a\\b');

        static::assertSame("'a\\b'", $column->default);
    }

    public function test_string_with_single_quote_is_escaped(): void
    {
        $column = schema_column('note', column_type_varchar(50), default: "it's");

        static::assertSame("'it''s'", $column->default);
    }
}
