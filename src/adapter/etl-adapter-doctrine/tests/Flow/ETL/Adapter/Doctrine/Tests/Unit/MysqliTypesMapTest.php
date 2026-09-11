<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\MysqliTypesMap;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;

use const MYSQLI_TYPE_BIT;
use const MYSQLI_TYPE_BLOB;
use const MYSQLI_TYPE_DATE;
use const MYSQLI_TYPE_DATETIME;
use const MYSQLI_TYPE_DECIMAL;
use const MYSQLI_TYPE_DOUBLE;
use const MYSQLI_TYPE_ENUM;
use const MYSQLI_TYPE_FLOAT;
use const MYSQLI_TYPE_GEOMETRY;
use const MYSQLI_TYPE_INT24;
use const MYSQLI_TYPE_JSON;
use const MYSQLI_TYPE_LONG;
use const MYSQLI_TYPE_LONGLONG;
use const MYSQLI_TYPE_NEWDATE;
use const MYSQLI_TYPE_NEWDECIMAL;
use const MYSQLI_TYPE_NULL;
use const MYSQLI_TYPE_SET;
use const MYSQLI_TYPE_SHORT;
use const MYSQLI_TYPE_STRING;
use const MYSQLI_TYPE_TIME;
use const MYSQLI_TYPE_TIMESTAMP;
use const MYSQLI_TYPE_TINY;
use const MYSQLI_TYPE_VAR_STRING;
use const MYSQLI_TYPE_YEAR;

final class MysqliTypesMapTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{int, Type<mixed>}>
     */
    public static function provide_mapped_constants(): Generator
    {
        yield 'TINY' => [MYSQLI_TYPE_TINY, type_integer()];
        yield 'SHORT' => [MYSQLI_TYPE_SHORT, type_integer()];
        yield 'INT24' => [MYSQLI_TYPE_INT24, type_integer()];
        yield 'LONG' => [MYSQLI_TYPE_LONG, type_integer()];
        yield 'LONGLONG' => [MYSQLI_TYPE_LONGLONG, type_integer()];
        yield 'YEAR' => [MYSQLI_TYPE_YEAR, type_integer()];
        yield 'FLOAT' => [MYSQLI_TYPE_FLOAT, type_float()];
        yield 'DOUBLE' => [MYSQLI_TYPE_DOUBLE, type_float()];
        yield 'DECIMAL' => [MYSQLI_TYPE_DECIMAL, type_float()];
        yield 'NEWDECIMAL' => [MYSQLI_TYPE_NEWDECIMAL, type_float()];
        yield 'STRING' => [MYSQLI_TYPE_STRING, type_string()];
        yield 'VAR_STRING' => [MYSQLI_TYPE_VAR_STRING, type_string()];
        yield 'BLOB' => [MYSQLI_TYPE_BLOB, type_string()];
        yield 'ENUM' => [MYSQLI_TYPE_ENUM, type_string()];
        yield 'SET' => [MYSQLI_TYPE_SET, type_string()];
        yield 'DATE' => [MYSQLI_TYPE_DATE, type_date()];
        yield 'NEWDATE' => [MYSQLI_TYPE_NEWDATE, type_date()];
        yield 'TIME' => [MYSQLI_TYPE_TIME, type_time()];
        yield 'DATETIME' => [MYSQLI_TYPE_DATETIME, type_datetime()];
        yield 'TIMESTAMP' => [MYSQLI_TYPE_TIMESTAMP, type_datetime()];
        yield 'JSON' => [MYSQLI_TYPE_JSON, type_json()];
    }

    /**
     * @param Type<mixed> $expected
     */
    #[DataProvider('provide_mapped_constants')]
    public function test_every_mapped_constant(int $mysqliType, Type $expected): void
    {
        static::assertEquals($expected, (new MysqliTypesMap())->toFlowType($mysqliType));
    }

    #[TestWith([MYSQLI_TYPE_BIT])]
    #[TestWith([MYSQLI_TYPE_GEOMETRY])]
    #[TestWith([MYSQLI_TYPE_NULL])]
    public function test_an_unmapped_constant_returns_null(int $mysqliType): void
    {
        static::assertNull((new MysqliTypesMap())->toFlowType($mysqliType));
    }

    public function test_tinyint_maps_to_integer_not_boolean(): void
    {
        static::assertEquals(type_integer(), (new MysqliTypesMap())->toFlowType(MYSQLI_TYPE_TINY));
    }
}
