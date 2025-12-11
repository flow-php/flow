<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\Schema\DataType;
use PHPUnit\Framework\TestCase;

final class DataTypeTest extends TestCase
{
    public function test_array_type() : void
    {
        $type = DataType::array(DataType::integer());

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertCount(1, $ast->getArrayBounds());
    }

    public function test_bigint() : void
    {
        $type = DataType::bigint();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('int8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bigserial() : void
    {
        $type = DataType::bigserial();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('bigserial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_boolean() : void
    {
        $type = DataType::boolean();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('bool', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_bytea() : void
    {
        $type = DataType::bytea();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('bytea', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_char_with_length() : void
    {
        $type = DataType::char(10);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('bpchar', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_cidr() : void
    {
        $type = DataType::cidr();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('cidr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_with_schema() : void
    {
        $type = DataType::custom('my_type', 'my_schema');

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('my_schema', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('my_type', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_custom_type_without_schema() : void
    {
        $type = DataType::custom('my_type');

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('my_type', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_date() : void
    {
        $type = DataType::date();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('date', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_double_precision() : void
    {
        $type = DataType::doublePrecision();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('float8', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_inet() : void
    {
        $type = DataType::inet();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('inet', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_integer() : void
    {
        $type = DataType::integer();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(2, $ast->getNames());
        self::assertSame('pg_catalog', $ast->getNames()[0]->getString()->getSval());
        self::assertSame('int4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_interval() : void
    {
        $type = DataType::interval();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('interval', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_json() : void
    {
        $type = DataType::json();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('json', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_jsonb() : void
    {
        $type = DataType::jsonb();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('jsonb', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_macaddr() : void
    {
        $type = DataType::macaddr();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('macaddr', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_numeric_with_precision_and_scale() : void
    {
        $type = DataType::numeric(10, 2);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(2, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
        self::assertSame(2, $ast->getTypmods()[1]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_with_precision_only() : void
    {
        $type = DataType::numeric(10);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(10, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_numeric_without_precision() : void
    {
        $type = DataType::numeric();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('numeric', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_real() : void
    {
        $type = DataType::real();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('float4', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_serial() : void
    {
        $type = DataType::serial();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertCount(1, $ast->getNames());
        self::assertSame('serial', $ast->getNames()[0]->getString()->getSval());
    }

    public function test_smallint() : void
    {
        $type = DataType::smallint();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('int2', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_text() : void
    {
        $type = DataType::text();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('text', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_time_with_precision() : void
    {
        $type = DataType::time(3);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_time_without_precision() : void
    {
        $type = DataType::time();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('time', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamp_with_precision() : void
    {
        $type = DataType::timestamp(6);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(6, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_timestamp_without_precision() : void
    {
        $type = DataType::timestamp();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamp', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamptz_with_precision() : void
    {
        $type = DataType::timestamptz(3);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('timestamptz', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(3, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }

    public function test_uuid() : void
    {
        $type = DataType::uuid();

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('uuid', $ast->getNames()[1]->getString()->getSval());
    }

    public function test_varchar_with_length() : void
    {
        $type = DataType::varchar(255);

        $ast = $type->toAst();

        self::assertInstanceOf(TypeName::class, $ast);
        self::assertSame('varchar', $ast->getNames()[1]->getString()->getSval());
        self::assertCount(1, $ast->getTypmods());
        self::assertSame(255, $ast->getTypmods()[0]->getAConst()->getIval()->getIval());
    }
}
