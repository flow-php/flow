<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_from_string;
use function Flow\Types\DSL\type_instance_of;

final class ColumnTypeParserTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_parse_bigint(): void
    {
        static::assertSame(
            ColumnType::bigint()->toAst()->serializeToString(),
            column_type_from_string('bigint')->toAst()->serializeToString(),
        );
    }

    public function test_parse_boolean(): void
    {
        static::assertSame(
            ColumnType::boolean()->toAst()->serializeToString(),
            column_type_from_string('boolean')->toAst()->serializeToString(),
        );
    }

    public function test_parse_bytea(): void
    {
        $ast = column_type_from_string('bytea')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('bytea', $string->getSval());
    }

    public function test_parse_char_with_length(): void
    {
        static::assertSame(
            ColumnType::char(10)->toAst()->serializeToString(),
            column_type_from_string('character(10)')->toAst()->serializeToString(),
        );
    }

    public function test_parse_cidr(): void
    {
        $ast = column_type_from_string('cidr')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('cidr', $string->getSval());
    }

    public function test_parse_custom_type(): void
    {
        $ast = column_type_from_string('my_custom_type')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('my_custom_type', $string->getSval());
    }

    public function test_parse_date(): void
    {
        $ast = column_type_from_string('date')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('date', $string->getSval());
    }

    public function test_parse_double_precision(): void
    {
        static::assertSame(
            ColumnType::doublePrecision()->toAst()->serializeToString(),
            column_type_from_string('double precision')->toAst()->serializeToString(),
        );
    }

    public function test_parse_empty_throws_exception(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        column_type_from_string('');
    }

    public function test_parse_inet(): void
    {
        $ast = column_type_from_string('inet')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('inet', $string->getSval());
    }

    public function test_parse_integer(): void
    {
        static::assertSame(
            ColumnType::integer()->toAst()->serializeToString(),
            column_type_from_string('integer')->toAst()->serializeToString(),
        );
    }

    public function test_parse_integer_array(): void
    {
        $ast = column_type_from_string('integer[]')->toAst();
        $names = $ast->getNames();

        static::assertCount(2, $names);
        $string0 = type_instance_of(PBString::class)->assert($names[0]->getString());
        $string1 = type_instance_of(PBString::class)->assert($names[1]->getString());
        static::assertSame('pg_catalog', $string0->getSval());
        static::assertSame('int4', $string1->getSval());
        static::assertCount(1, $ast->getArrayBounds());
    }

    public function test_parse_interval(): void
    {
        static::assertSame(
            ColumnType::interval()->toAst()->serializeToString(),
            column_type_from_string('interval')->toAst()->serializeToString(),
        );
    }

    public function test_parse_json(): void
    {
        static::assertSame(
            ColumnType::json()->toAst()->serializeToString(),
            column_type_from_string('json')->toAst()->serializeToString(),
        );
    }

    public function test_parse_jsonb(): void
    {
        $ast = column_type_from_string('jsonb')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('jsonb', $string->getSval());
    }

    public function test_parse_macaddr(): void
    {
        $ast = column_type_from_string('macaddr')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('macaddr', $string->getSval());
    }

    public function test_parse_numeric(): void
    {
        static::assertSame(
            ColumnType::numeric()->toAst()->serializeToString(),
            column_type_from_string('numeric')->toAst()->serializeToString(),
        );
    }

    public function test_parse_numeric_with_precision_and_scale(): void
    {
        static::assertSame(
            ColumnType::numeric(10, 2)->toAst()->serializeToString(),
            column_type_from_string('numeric(10,2)')->toAst()->serializeToString(),
        );
    }

    public function test_parse_real(): void
    {
        static::assertSame(
            ColumnType::real()->toAst()->serializeToString(),
            column_type_from_string('real')->toAst()->serializeToString(),
        );
    }

    public function test_parse_schema_qualified_custom_type(): void
    {
        $ast = column_type_from_string('my_schema.my_type')->toAst();
        $names = $ast->getNames();

        static::assertCount(2, $names);
        $string0 = type_instance_of(PBString::class)->assert($names[0]->getString());
        $string1 = type_instance_of(PBString::class)->assert($names[1]->getString());
        static::assertSame('my_schema', $string0->getSval());
        static::assertSame('my_type', $string1->getSval());
    }

    public function test_parse_smallint(): void
    {
        static::assertSame(
            ColumnType::smallint()->toAst()->serializeToString(),
            column_type_from_string('smallint')->toAst()->serializeToString(),
        );
    }

    public function test_parse_text(): void
    {
        $ast = column_type_from_string('text')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('text', $string->getSval());
    }

    public function test_parse_text_array(): void
    {
        $ast = column_type_from_string('text[]')->toAst();
        $names = $ast->getNames();

        static::assertCount(1, $names);
        $string = type_instance_of(PBString::class)->assert($names[0]->getString());
        static::assertSame('text', $string->getSval());
        static::assertCount(1, $ast->getArrayBounds());
    }

    public function test_parse_time_with_precision(): void
    {
        static::assertSame(
            ColumnType::time(3)->toAst()->serializeToString(),
            column_type_from_string('time(3) without time zone')->toAst()->serializeToString(),
        );
    }

    public function test_parse_timestamp(): void
    {
        static::assertSame(
            ColumnType::timestamp()->toAst()->serializeToString(),
            column_type_from_string('timestamp without time zone')->toAst()->serializeToString(),
        );
    }

    public function test_parse_timestamp_with_precision(): void
    {
        static::assertSame(
            ColumnType::timestamp(6)->toAst()->serializeToString(),
            column_type_from_string('timestamp(6) without time zone')->toAst()->serializeToString(),
        );
    }

    public function test_parse_timestamptz(): void
    {
        static::assertSame(
            ColumnType::timestamptz()->toAst()->serializeToString(),
            column_type_from_string('timestamp with time zone')->toAst()->serializeToString(),
        );
    }

    public function test_parse_timestamptz_with_precision(): void
    {
        static::assertSame(
            ColumnType::timestamptz(3)->toAst()->serializeToString(),
            column_type_from_string('timestamp(3) with time zone')->toAst()->serializeToString(),
        );
    }

    public function test_parse_uuid(): void
    {
        $ast = column_type_from_string('uuid')->toAst();

        static::assertCount(1, $ast->getNames());
        $string = type_instance_of(PBString::class)->assert($ast->getNames()[0]->getString());
        static::assertSame('uuid', $string->getSval());
    }

    public function test_parse_varchar_with_length(): void
    {
        static::assertSame(
            ColumnType::varchar(255)->toAst()->serializeToString(),
            column_type_from_string('character varying(255)')->toAst()->serializeToString(),
        );
    }
}
