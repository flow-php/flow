<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use function Flow\PostgreSql\DSL\column_type_from_string;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class ColumnTypeParserTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_parse_bigint() : void
    {
        self::assertSame(
            ColumnType::bigint()->toAst()->serializeToString(),
            column_type_from_string('bigint')->toAst()->serializeToString()
        );
    }

    public function test_parse_boolean() : void
    {
        self::assertSame(
            ColumnType::boolean()->toAst()->serializeToString(),
            column_type_from_string('boolean')->toAst()->serializeToString()
        );
    }

    public function test_parse_bytea() : void
    {
        $ast = column_type_from_string('bytea')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('bytea', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_char_with_length() : void
    {
        self::assertSame(
            ColumnType::char(10)->toAst()->serializeToString(),
            column_type_from_string('character(10)')->toAst()->serializeToString()
        );
    }

    public function test_parse_cidr() : void
    {
        $ast = column_type_from_string('cidr')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('cidr', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_custom_type() : void
    {
        $ast = column_type_from_string('my_custom_type')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('my_custom_type', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_date() : void
    {
        $ast = column_type_from_string('date')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('date', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_double_precision() : void
    {
        self::assertSame(
            ColumnType::doublePrecision()->toAst()->serializeToString(),
            column_type_from_string('double precision')->toAst()->serializeToString()
        );
    }

    public function test_parse_empty_throws_exception() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        column_type_from_string('');
    }

    public function test_parse_inet() : void
    {
        $ast = column_type_from_string('inet')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('inet', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_integer() : void
    {
        self::assertSame(
            ColumnType::integer()->toAst()->serializeToString(),
            column_type_from_string('integer')->toAst()->serializeToString()
        );
    }

    public function test_parse_integer_array() : void
    {
        $ast = column_type_from_string('integer[]')->toAst();
        $names = $ast->getNames();

        self::assertCount(2, $names);
        self::assertSame('pg_catalog', $names[0]->getString()->getSval());
        self::assertSame('int4', $names[1]->getString()->getSval());
        self::assertCount(1, $ast->getArrayBounds());
    }

    public function test_parse_interval() : void
    {
        self::assertSame(
            ColumnType::interval()->toAst()->serializeToString(),
            column_type_from_string('interval')->toAst()->serializeToString()
        );
    }

    public function test_parse_json() : void
    {
        self::assertSame(
            ColumnType::json()->toAst()->serializeToString(),
            column_type_from_string('json')->toAst()->serializeToString()
        );
    }

    public function test_parse_jsonb() : void
    {
        $ast = column_type_from_string('jsonb')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('jsonb', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_macaddr() : void
    {
        $ast = column_type_from_string('macaddr')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('macaddr', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_numeric() : void
    {
        self::assertSame(
            ColumnType::numeric()->toAst()->serializeToString(),
            column_type_from_string('numeric')->toAst()->serializeToString()
        );
    }

    public function test_parse_numeric_with_precision_and_scale() : void
    {
        self::assertSame(
            ColumnType::numeric(10, 2)->toAst()->serializeToString(),
            column_type_from_string('numeric(10,2)')->toAst()->serializeToString()
        );
    }

    public function test_parse_real() : void
    {
        self::assertSame(
            ColumnType::real()->toAst()->serializeToString(),
            column_type_from_string('real')->toAst()->serializeToString()
        );
    }

    public function test_parse_schema_qualified_custom_type() : void
    {
        $ast = column_type_from_string('my_schema.my_type')->toAst();
        $names = $ast->getNames();

        self::assertCount(2, $names);
        self::assertSame('my_schema', $names[0]->getString()->getSval());
        self::assertSame('my_type', $names[1]->getString()->getSval());
    }

    public function test_parse_smallint() : void
    {
        self::assertSame(
            ColumnType::smallint()->toAst()->serializeToString(),
            column_type_from_string('smallint')->toAst()->serializeToString()
        );
    }

    public function test_parse_text() : void
    {
        $ast = column_type_from_string('text')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('text', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_text_array() : void
    {
        $ast = column_type_from_string('text[]')->toAst();
        $names = $ast->getNames();

        self::assertCount(1, $names);
        self::assertSame('text', $names[0]->getString()->getSval());
        self::assertCount(1, $ast->getArrayBounds());
    }

    public function test_parse_time_with_precision() : void
    {
        self::assertSame(
            ColumnType::time(3)->toAst()->serializeToString(),
            column_type_from_string('time(3) without time zone')->toAst()->serializeToString()
        );
    }

    public function test_parse_timestamp() : void
    {
        self::assertSame(
            ColumnType::timestamp()->toAst()->serializeToString(),
            column_type_from_string('timestamp without time zone')->toAst()->serializeToString()
        );
    }

    public function test_parse_timestamp_with_precision() : void
    {
        self::assertSame(
            ColumnType::timestamp(6)->toAst()->serializeToString(),
            column_type_from_string('timestamp(6) without time zone')->toAst()->serializeToString()
        );
    }

    public function test_parse_timestamptz() : void
    {
        self::assertSame(
            ColumnType::timestamptz()->toAst()->serializeToString(),
            column_type_from_string('timestamp with time zone')->toAst()->serializeToString()
        );
    }

    public function test_parse_timestamptz_with_precision() : void
    {
        self::assertSame(
            ColumnType::timestamptz(3)->toAst()->serializeToString(),
            column_type_from_string('timestamp(3) with time zone')->toAst()->serializeToString()
        );
    }

    public function test_parse_uuid() : void
    {
        $ast = column_type_from_string('uuid')->toAst();

        self::assertCount(1, $ast->getNames());
        self::assertSame('uuid', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_parse_varchar_with_length() : void
    {
        self::assertSame(
            ColumnType::varchar(255)->toAst()->serializeToString(),
            column_type_from_string('character varying(255)')->toAst()->serializeToString()
        );
    }
}
