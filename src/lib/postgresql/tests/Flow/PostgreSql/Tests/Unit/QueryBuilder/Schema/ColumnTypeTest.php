<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_array;
use function Flow\PostgreSql\DSL\column_type_bigint;
use function Flow\PostgreSql\DSL\column_type_boolean;
use function Flow\PostgreSql\DSL\column_type_custom;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_varchar;

final class ColumnTypeTest extends TestCase
{
    public function test_array_type(): void
    {
        $type = ColumnType::array(ColumnType::integer());

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        static::assertCount(1, $ast->getArrayBounds());
    }

    public function test_bigint(): void
    {
        $type = ColumnType::bigint();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        static::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        static::assertSame('int8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bigserial(): void
    {
        $type = ColumnType::bigserial();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        static::assertSame('bigserial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_boolean(): void
    {
        $type = ColumnType::boolean();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        static::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        static::assertSame('bool', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bytea(): void
    {
        $type = ColumnType::bytea();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('bytea', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_char_with_length(): void
    {
        $type = ColumnType::char(10);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('bpchar', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_cidr(): void
    {
        $type = ColumnType::cidr();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('cidr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_with_schema(): void
    {
        $type = ColumnType::custom('my_type', 'my_schema');

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        static::assertSame('my_schema', $ast->getNames()[0]->getString()->getSval());
        static::assertSame('my_type', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_without_schema(): void
    {
        $type = ColumnType::custom('my_type');

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        static::assertSame('my_type', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_date(): void
    {
        $type = ColumnType::date();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('date', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_double_precision(): void
    {
        $type = ColumnType::doublePrecision();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('float8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_inet(): void
    {
        $type = ColumnType::inet();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('inet', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_integer(): void
    {
        $type = ColumnType::integer();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        static::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        static::assertSame('int4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_interval(): void
    {
        $type = ColumnType::interval();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('interval', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_is_equal_array_different_element_type(): void
    {
        static::assertFalse(column_type_array(column_type_integer())->isEqual(column_type_array(column_type_text())));
    }

    public function test_is_equal_array_same_element_type(): void
    {
        static::assertTrue(column_type_array(column_type_integer())->isEqual(column_type_array(column_type_integer())));
    }

    public function test_is_equal_array_vs_non_array(): void
    {
        static::assertFalse(column_type_array(column_type_integer())->isEqual(column_type_integer()));
    }

    public function test_is_equal_boolean_vs_bigint(): void
    {
        static::assertFalse(column_type_boolean()->isEqual(column_type_bigint()));
    }

    public function test_is_equal_custom_types_different_name(): void
    {
        static::assertFalse(column_type_custom('type_a')->isEqual(column_type_custom('type_b')));
    }

    public function test_is_equal_custom_types_different_schema(): void
    {
        static::assertFalse(column_type_custom('my_type', 'schema_a')->isEqual(column_type_custom(
            'my_type',
            'schema_b',
        )));
    }

    public function test_is_equal_custom_types_same(): void
    {
        static::assertTrue(column_type_custom('my_type', 'my_schema')->isEqual(column_type_custom(
            'my_type',
            'my_schema',
        )));
    }

    public function test_is_equal_with_different_types(): void
    {
        static::assertFalse(column_type_integer()->isEqual(column_type_text()));
    }

    public function test_is_equal_with_different_typmods(): void
    {
        static::assertFalse(column_type_varchar(100)->isEqual(column_type_varchar(255)));
    }

    public function test_is_equal_with_same_type(): void
    {
        static::assertTrue(column_type_integer()->isEqual(column_type_integer()));
    }

    public function test_is_equal_with_same_type_and_typmods(): void
    {
        static::assertTrue(column_type_varchar(255)->isEqual(column_type_varchar(255)));
    }

    public function test_json(): void
    {
        $type = ColumnType::json();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('json', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_jsonb(): void
    {
        $type = ColumnType::jsonb();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('jsonb', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_macaddr(): void
    {
        $type = ColumnType::macaddr();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('macaddr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_numeric_with_precision_and_scale(): void
    {
        $type = ColumnType::numeric(10, 2);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(2, $ast->getTypmods());
        static::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
        static::assertSame(2, $ast->getTypmods()[1]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_with_precision_only(): void
    {
        $type = ColumnType::numeric(10);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_without_precision(): void
    {
        $type = ColumnType::numeric();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_real(): void
    {
        $type = ColumnType::real();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('float4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_serial(): void
    {
        $type = ColumnType::serial();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        static::assertSame('serial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_smallint(): void
    {
        $type = ColumnType::smallint();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('int2', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_text(): void
    {
        $type = ColumnType::text();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('text', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_time_with_precision(): void
    {
        $type = ColumnType::time(3);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_time_without_precision(): void
    {
        $type = ColumnType::time();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamp_with_precision(): void
    {
        $type = ColumnType::timestamp(6);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(6, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_timestamp_without_precision(): void
    {
        $type = ColumnType::timestamp();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamptz_with_precision(): void
    {
        $type = ColumnType::timestamptz(3);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('timestamptz', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_uuid(): void
    {
        $type = ColumnType::uuid();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('uuid', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_varchar_with_length(): void
    {
        $type = ColumnType::varchar(255);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertSame('varchar', $ast->getNames()[1]->getString()->getSval());
        static::assertCount(1, $ast->getTypmods());
        static::assertSame(255, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }
}
