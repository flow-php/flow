<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use function Flow\PostgreSql\DSL\{column_type_varchar, func, literal, schema_column, schema_column_boolean, schema_column_integer, schema_column_real, schema_column_text, schema_column_timestamp_tz, schema_column_uuid, schema_column_varchar, schema_table};

use PHPUnit\Framework\TestCase;

final class SchemaColumnDefaultTest extends TestCase
{
    public function test_bool_false_default_is_stored_as_literal() : void
    {
        $column = schema_column_boolean('active', default: false);

        self::assertSame('false', $column->default);
    }

    public function test_bool_true_default_is_stored_as_literal() : void
    {
        $column = schema_column_boolean('active', default: true);

        self::assertSame('true', $column->default);
    }

    public function test_float_default_is_stored_as_literal() : void
    {
        $column = schema_column_real('price', default: 3.14);

        self::assertSame('3.14', $column->default);
    }

    public function test_func_call_expression_is_stored_as_deparsed_sql() : void
    {
        $column = schema_column_timestamp_tz('created_at', default: func('now'));

        self::assertSame('now()', $column->default);
    }

    public function test_func_call_uuid_default() : void
    {
        $column = schema_column_uuid('external_id', default: func('gen_random_uuid'));

        self::assertSame('gen_random_uuid()', $column->default);
    }

    public function test_int_default_is_stored_as_literal() : void
    {
        self::assertSame('0', schema_column_integer('age', default: 0)->default);
        self::assertSame('-42', schema_column_integer('delta', default: -42)->default);
    }

    public function test_literal_expression_matches_plain_scalar_equivalent() : void
    {
        self::assertSame(
            schema_column('status', column_type_varchar(50), default: 'active')->default,
            schema_column('status', column_type_varchar(50), default: literal('active'))->default,
        );
        self::assertSame(
            schema_column_integer('age', default: 0)->default,
            schema_column_integer('age', default: literal(0))->default,
        );
    }

    public function test_negative_float_default_is_stored_as_literal() : void
    {
        $column = schema_column_real('delta', default: -2.5);

        self::assertSame('-2.5', $column->default);
    }

    public function test_null_default_is_stored_as_null() : void
    {
        self::assertNull(schema_column('name', column_type_varchar(50))->default);
        self::assertNull(schema_column('name', column_type_varchar(50), default: null)->default);
    }

    public function test_plain_string_default_is_stored_as_pg_quoted_literal() : void
    {
        $column = schema_column('status', column_type_varchar(50), default: 'active');

        self::assertSame("'active'", $column->default);
    }

    public function test_round_trip_through_to_sql() : void
    {
        $table = schema_table('events', [
            schema_column_varchar('status', 50, nullable: false, default: 'active'),
            schema_column_timestamp_tz('created_at', nullable: false, default: func('now')),
            schema_column_integer('priority', nullable: false, default: 0),
        ]);

        $statements = $table->toSql();
        $sql = \implode("\n", \array_map(static fn ($stmt) => $stmt->toSql(), $statements));

        self::assertStringContainsString("DEFAULT 'active'", $sql);
        self::assertStringContainsString('DEFAULT now()', $sql);
        self::assertStringContainsString('DEFAULT 0', $sql);
    }

    public function test_schema_column_text_accepts_typed_string_default() : void
    {
        $column = schema_column_text('label', default: 'unknown');

        self::assertSame("'unknown'", $column->default);
    }

    public function test_schema_column_varchar_accepts_typed_default() : void
    {
        $column = schema_column_varchar('status', 50, default: 'pending');

        self::assertSame("'pending'", $column->default);
    }

    public function test_string_with_backslash_is_stored_verbatim_inside_literal() : void
    {
        $column = schema_column('path', column_type_varchar(50), default: 'a\\b');

        self::assertSame("'a\\b'", $column->default);
    }

    public function test_string_with_single_quote_is_escaped() : void
    {
        $column = schema_column('note', column_type_varchar(50), default: "it's");

        self::assertSame("'it''s'", $column->default);
    }
}
