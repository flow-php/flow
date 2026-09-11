<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\PgSqlTypesMap;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class PgSqlTypesMapTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string, Type<mixed>}>
     */
    public static function provide_mapped_types(): Generator
    {
        yield 'int2' => ['int2', type_integer()];
        yield 'int4' => ['int4', type_integer()];
        yield 'int8' => ['int8', type_integer()];
        yield 'float4' => ['float4', type_float()];
        yield 'float8' => ['float8', type_float()];
        yield 'numeric' => ['numeric', type_float()];
        yield 'bool' => ['bool', type_boolean()];
        yield 'text' => ['text', type_string()];
        yield 'varchar' => ['varchar', type_string()];
        yield 'bpchar' => ['bpchar', type_string()];
        yield 'name' => ['name', type_string()];
        yield 'bytea' => ['bytea', type_string()];
        yield 'date' => ['date', type_date()];
        yield 'time' => ['time', type_time()];
        yield 'timestamp' => ['timestamp', type_datetime()];
        yield 'timestamptz' => ['timestamptz', type_datetime()];
        yield 'uuid' => ['uuid', type_uuid()];
        yield 'json' => ['json', type_json()];
        yield 'jsonb' => ['jsonb', type_json()];
        yield 'xml' => ['xml', type_xml()];
    }

    /**
     * @param Type<mixed> $expected
     */
    #[DataProvider('provide_mapped_types')]
    public function test_every_mapped_type(string $pgType, Type $expected): void
    {
        static::assertEquals($expected, (new PgSqlTypesMap())->toFlowType($pgType));
    }

    #[TestWith(['_int4'])]
    #[TestWith(['money'])]
    #[TestWith(['inet'])]
    #[TestWith(['cidr'])]
    #[TestWith(['macaddr'])]
    #[TestWith(['interval'])]
    #[TestWith(['timetz'])]
    #[TestWith(['int4range'])]
    #[TestWith(['point'])]
    #[TestWith(['oid'])]
    public function test_an_unmapped_type_returns_null(string $pgType): void
    {
        static::assertNull((new PgSqlTypesMap())->toFlowType($pgType));
    }
}
