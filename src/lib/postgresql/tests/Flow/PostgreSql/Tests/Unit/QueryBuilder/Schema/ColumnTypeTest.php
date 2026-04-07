<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use function Flow\PostgreSql\DSL\{column_type_array, column_type_bigint, column_type_boolean, column_type_custom, column_type_integer, column_type_text, column_type_varchar};

use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class ColumnTypeTest extends TestCase
{
    public function test_array_type() : void
    {
        $type = ColumnType::array(ColumnType::integer());

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertCount(1, $ast->getArrayBounds());
    }

    public function test_bigint() : void
    {
        $type = ColumnType::bigint();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('int8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bigserial() : void
    {
        $type = ColumnType::bigserial();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('bigserial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_boolean() : void
    {
        $type = ColumnType::boolean();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('bool', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bytea() : void
    {
        $type = ColumnType::bytea();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('bytea', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_char_with_length() : void
    {
        $type = ColumnType::char(10);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('bpchar', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_cidr() : void
    {
        $type = ColumnType::cidr();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('cidr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_with_schema() : void
    {
        $type = ColumnType::custom('my_type', 'my_schema');

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('my_schema', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('my_type', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_without_schema() : void
    {
        $type = ColumnType::custom('my_type');

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('my_type', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_date() : void
    {
        $type = ColumnType::date();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('date', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_double_precision() : void
    {
        $type = ColumnType::doublePrecision();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('float8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_inet() : void
    {
        $type = ColumnType::inet();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('inet', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_integer() : void
    {
        $type = ColumnType::integer();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('int4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_interval() : void
    {
        $type = ColumnType::interval();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('interval', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_is_equal_array_different_element_type() : void
    {
        self::assertFalse(column_type_array(column_type_integer())->isEqual(column_type_array(column_type_text())));
    }

    public function test_is_equal_array_same_element_type() : void
    {
        self::assertTrue(column_type_array(column_type_integer())->isEqual(column_type_array(column_type_integer())));
    }

    public function test_is_equal_array_vs_non_array() : void
    {
        self::assertFalse(column_type_array(column_type_integer())->isEqual(column_type_integer()));
    }

    public function test_is_equal_boolean_vs_bigint() : void
    {
        self::assertFalse(column_type_boolean()->isEqual(column_type_bigint()));
    }

    public function test_is_equal_custom_types_different_name() : void
    {
        self::assertFalse(column_type_custom('type_a')->isEqual(column_type_custom('type_b')));
    }

    public function test_is_equal_custom_types_different_schema() : void
    {
        self::assertFalse(column_type_custom('my_type', 'schema_a')->isEqual(column_type_custom('my_type', 'schema_b')));
    }

    public function test_is_equal_custom_types_same() : void
    {
        self::assertTrue(column_type_custom('my_type', 'my_schema')->isEqual(column_type_custom('my_type', 'my_schema')));
    }

    public function test_is_equal_with_different_types() : void
    {
        self::assertFalse(column_type_integer()->isEqual(column_type_text()));
    }

    public function test_is_equal_with_different_typmods() : void
    {
        self::assertFalse(column_type_varchar(100)->isEqual(column_type_varchar(255)));
    }

    public function test_is_equal_with_same_type() : void
    {
        self::assertTrue(column_type_integer()->isEqual(column_type_integer()));
    }

    public function test_is_equal_with_same_type_and_typmods() : void
    {
        self::assertTrue(column_type_varchar(255)->isEqual(column_type_varchar(255)));
    }

    public function test_json() : void
    {
        $type = ColumnType::json();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('json', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_jsonb() : void
    {
        $type = ColumnType::jsonb();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('jsonb', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_macaddr() : void
    {
        $type = ColumnType::macaddr();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('macaddr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_numeric_with_precision_and_scale() : void
    {
        $type = ColumnType::numeric(10, 2);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(2, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
        self::assertSame(2, $ast->getTypmods()[1]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_with_precision_only() : void
    {
        $type = ColumnType::numeric(10);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_without_precision() : void
    {
        $type = ColumnType::numeric();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_real() : void
    {
        $type = ColumnType::real();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('float4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_serial() : void
    {
        $type = ColumnType::serial();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('serial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_smallint() : void
    {
        $type = ColumnType::smallint();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('int2', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_text() : void
    {
        $type = ColumnType::text();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('text', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_time_with_precision() : void
    {
        $type = ColumnType::time(3);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_time_without_precision() : void
    {
        $type = ColumnType::time();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamp_with_precision() : void
    {
        $type = ColumnType::timestamp(6);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(6, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_timestamp_without_precision() : void
    {
        $type = ColumnType::timestamp();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamptz_with_precision() : void
    {
        $type = ColumnType::timestamptz(3);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamptz', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_uuid() : void
    {
        $type = ColumnType::uuid();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('uuid', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_varchar_with_length() : void
    {
        $type = ColumnType::varchar(255);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('varchar', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(255, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }
}
