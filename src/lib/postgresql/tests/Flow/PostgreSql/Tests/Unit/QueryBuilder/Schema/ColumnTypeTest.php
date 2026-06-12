<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\PBString;
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
use function Flow\Types\DSL\type_instance_of;

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
        $catalog = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $catalog);
        static::assertSame('pg_catalog', $catalog->getSval());
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('int8', $name->getSval());
    }

    public function test_bigserial(): void
    {
        $type = ColumnType::bigserial();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        $name = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('bigserial', $name->getSval());
    }

    public function test_boolean(): void
    {
        $type = ColumnType::boolean();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        $catalog = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $catalog);
        static::assertSame('pg_catalog', $catalog->getSval());
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('bool', $name->getSval());
    }

    public function test_bytea(): void
    {
        $type = ColumnType::bytea();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('bytea', $name->getSval());
    }

    public function test_char_with_length(): void
    {
        $type = ColumnType::char(10);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('bpchar', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $aConst = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($aConst);
        static::assertSame(10, type_instance_of(Integer::class)->assert($aConst->getIval())->getIval());
    }

    public function test_cidr(): void
    {
        $type = ColumnType::cidr();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('cidr', $name->getSval());
    }

    public function test_custom_type_with_schema(): void
    {
        $type = ColumnType::custom('my_type', 'my_schema');

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        $schema = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $schema);
        static::assertSame('my_schema', $schema->getSval());
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('my_type', $name->getSval());
    }

    public function test_custom_type_without_schema(): void
    {
        $type = ColumnType::custom('my_type');

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        $name = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('my_type', $name->getSval());
    }

    public function test_date(): void
    {
        $type = ColumnType::date();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('date', $name->getSval());
    }

    public function test_double_precision(): void
    {
        $type = ColumnType::doublePrecision();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('float8', $name->getSval());
    }

    public function test_inet(): void
    {
        $type = ColumnType::inet();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('inet', $name->getSval());
    }

    public function test_integer(): void
    {
        $type = ColumnType::integer();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        $catalog = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $catalog);
        static::assertSame('pg_catalog', $catalog->getSval());
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('int4', $name->getSval());
    }

    public function test_interval(): void
    {
        $type = ColumnType::interval();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('interval', $name->getSval());
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

    public function test_is_same_base_type_array_vs_scalar(): void
    {
        static::assertFalse(ColumnType::array(column_type_text())->isSameBaseType(column_type_text()));
    }

    public function test_is_same_base_type_different_base_types(): void
    {
        static::assertFalse(ColumnType::numeric(10, 3)->isSameBaseType(ColumnType::doublePrecision()));
        static::assertFalse(column_type_integer()->isSameBaseType(column_type_bigint()));
    }

    public function test_is_same_base_type_ignores_typmods(): void
    {
        static::assertTrue(ColumnType::numeric(10, 3)->isSameBaseType(ColumnType::numeric()));
        static::assertTrue(column_type_varchar(255)->isSameBaseType(column_type_varchar(100)));
    }

    public function test_is_same_base_type_serial_normalizes_to_integer(): void
    {
        static::assertTrue(ColumnType::serial()->isSameBaseType(column_type_integer()));
    }

    public function test_json(): void
    {
        $type = ColumnType::json();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('json', $name->getSval());
    }

    public function test_jsonb(): void
    {
        $type = ColumnType::jsonb();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('jsonb', $name->getSval());
    }

    public function test_macaddr(): void
    {
        $type = ColumnType::macaddr();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('macaddr', $name->getSval());
    }

    public function test_numeric_with_precision_and_scale(): void
    {
        $type = ColumnType::numeric(10, 2);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('numeric', $name->getSval());
        static::assertCount(2, $ast->getTypmods());
        $precision = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($precision);
        static::assertSame(10, type_instance_of(Integer::class)->assert($precision->getIval())->getIval());
        $scale = $ast->getTypmods()[1]->getAConst();
        static::assertNotNull($scale);
        static::assertSame(2, type_instance_of(Integer::class)->assert($scale->getIval())->getIval());
    }

    public function test_numeric_with_precision_only(): void
    {
        $type = ColumnType::numeric(10);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('numeric', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $precision = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($precision);
        static::assertSame(10, type_instance_of(Integer::class)->assert($precision->getIval())->getIval());
    }

    public function test_numeric_without_precision(): void
    {
        $type = ColumnType::numeric();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('numeric', $name->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_real(): void
    {
        $type = ColumnType::real();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('float4', $name->getSval());
    }

    public function test_serial(): void
    {
        $type = ColumnType::serial();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(1, $ast->getNames());
        $name = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('serial', $name->getSval());
    }

    public function test_smallint(): void
    {
        $type = ColumnType::smallint();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('int2', $name->getSval());
    }

    public function test_text(): void
    {
        $type = ColumnType::text();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('text', $name->getSval());
    }

    public function test_time_with_precision(): void
    {
        $type = ColumnType::time(3);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('time', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $precision = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($precision);
        static::assertSame(3, type_instance_of(Integer::class)->assert($precision->getIval())->getIval());
    }

    public function test_time_without_precision(): void
    {
        $type = ColumnType::time();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('time', $name->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamp_with_precision(): void
    {
        $type = ColumnType::timestamp(6);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('timestamp', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $precision = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($precision);
        static::assertSame(6, type_instance_of(Integer::class)->assert($precision->getIval())->getIval());
    }

    public function test_timestamp_without_precision(): void
    {
        $type = ColumnType::timestamp();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('timestamp', $name->getSval());
        static::assertCount(0, $ast->getTypmods());
    }

    public function test_timestamptz_with_precision(): void
    {
        $type = ColumnType::timestamptz(3);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('timestamptz', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $precision = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($precision);
        static::assertSame(3, type_instance_of(Integer::class)->assert($precision->getIval())->getIval());
    }

    public function test_uuid(): void
    {
        $type = ColumnType::uuid();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('uuid', $name->getSval());
    }

    public function test_varchar_with_length(): void
    {
        $type = ColumnType::varchar(255);

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('varchar', $name->getSval());
        static::assertCount(1, $ast->getTypmods());
        $length = $ast->getTypmods()[0]->getAConst();
        static::assertNotNull($length);
        static::assertSame(255, type_instance_of(Integer::class)->assert($length->getIval())->getIval());
    }

    public function test_xml(): void
    {
        $type = ColumnType::xml();

        $ast = $type->toAst();

        static::assertInstanceOf(TypeName::class, $ast);
        static::assertCount(2, $ast->getNames());
        $catalog = $ast->getNames()[0]->getString();
        static::assertInstanceOf(PBString::class, $catalog);
        static::assertSame('pg_catalog', $catalog->getSval());
        $name = $ast->getNames()[1]->getString();
        static::assertInstanceOf(PBString::class, $name);
        static::assertSame('xml', $name->getSval());
    }
}
