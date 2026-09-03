<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\MysqliTypesMap;
use Flow\ETL\Adapter\Doctrine\PgSqlTypesMap;
use Flow\ETL\Adapter\Doctrine\ResultColumn;
use Flow\ETL\Adapter\Doctrine\SqliteTypes;
use Flow\ETL\Adapter\Doctrine\TypedColumns;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

use const MYSQLI_TYPE_BIT;
use const MYSQLI_TYPE_LONG;

final class TypedColumnsTest extends FlowTestCase
{
    public function test_a_driver_type_without_a_flow_type_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('column "tags" has driver type "_text", which Flow has no type for');

        (new TypedColumns())->schema(
            [new ResultColumn('id', 'int4'), new ResultColumn('tags', '_text')],
            new PgSqlTypesMap(),
            self::class,
        );
    }

    public function test_a_mysqli_constant_without_a_flow_type_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('column "flags" has driver type "16", which Flow has no type for');

        (new TypedColumns())->schema([new ResultColumn('flags', MYSQLI_TYPE_BIT)], new MysqliTypesMap(), self::class);
    }

    public function test_duplicate_output_names_collapse_last_wins(): void
    {
        $schema = (new TypedColumns())->schema(
            [new ResultColumn('a', 'int4'), new ResultColumn('a', 'text')],
            new PgSqlTypesMap(),
            self::class,
        );

        static::assertSame(['a'], $schema->references()->names());
        static::assertEquals(type_string(), $schema->findDefinition('a')?->type());
    }

    public function test_every_column_is_nullable(): void
    {
        $schema = (new TypedColumns())->schema(
            [new ResultColumn('id', MYSQLI_TYPE_LONG), new ResultColumn('name', MYSQLI_TYPE_LONG)],
            new MysqliTypesMap(),
            self::class,
        );

        foreach ($schema->definitions() as $definition) {
            static::assertTrue($definition->isNullable());
        }

        static::assertEquals(type_integer(), $schema->findDefinition('id')?->type());
    }

    public function test_sqlite_columns_carry_no_native_type_and_describe_as_string(): void
    {
        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true)),
            (new TypedColumns())->schema(
                [new ResultColumn('id'), new ResultColumn('name')],
                new SqliteTypes(),
                self::class,
            ),
        );
    }
}
