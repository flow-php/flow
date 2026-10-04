<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ParquetSchemaConformance;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_list;

final class ParquetSchemaConformanceTest extends FlowTestCase
{
    public function test_a_column_of_another_type_is_cast_to_the_writer_type(): void
    {
        $conformed = (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(
            str_schema('id'),
            str_schema('name'),
        ))))->conform(
            array_to_rows(
                [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
                schema(int_schema('id'), str_schema('name')),
            ),
            new AdaptiveBackend(),
        );

        static::assertEquals(schema(str_schema('id'), str_schema('name')), $conformed->schema());
        static::assertSame(['1', '2'], $conformed->column('id')->values());
    }

    public function test_a_date_is_cast_to_a_datetime(): void
    {
        $conformed = (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(datetime_schema(
            'at',
        )))))->conform(array_to_rows([[
            'at' => new DateTimeImmutable('2026-09-01'),
        ]], schema(date_schema('at'))), new AdaptiveBackend());

        static::assertEquals(schema(datetime_schema('at')), $conformed->schema());
        static::assertEquals([new DateTimeImmutable('2026-09-01 00:00:00 UTC')], $conformed->column('at')->values());
    }

    public function test_a_null_typed_column_is_cast_to_a_string_column(): void
    {
        $conformed = (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(null_schema(
            'nothing',
        )))))->conform(array_to_rows([['nothing' => null]], schema(null_schema('nothing'))), new AdaptiveBackend());

        static::assertEquals(schema(str_schema('nothing', nullable: true)), $conformed->schema());
        static::assertSame([null], $conformed->column('nothing')->values());
    }

    public function test_a_matching_column_is_passed_on_as_it_is(): void
    {
        $rows = array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name')));

        $conformed = (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(
            str_schema('id'),
            str_schema('name'),
        ))))->conform($rows, new AdaptiveBackend());

        static::assertSame($rows->column('name'), $conformed->column('name'));
    }

    public function test_a_zoned_datetime_column_is_passed_on_as_it_is(): void
    {
        $schema = schema(
            datetime_schema('at', zone: 'Europe/Warsaw'),
            list_schema('history', type_list(type_datetime('Asia/Tokyo'))),
        );
        $rows = array_to_rows([[
            'at' => new DateTimeImmutable('2026-09-01 10:00:00 UTC'),
            'history' => [new DateTimeImmutable('2026-09-01 11:00:00 UTC')],
        ]], $schema);

        static::assertSame($rows, (new ParquetSchemaConformance((new SchemaConverter())->toParquet($schema)))->conform(
            $rows,
            new AdaptiveBackend(),
        ));
    }

    public function test_rows_matching_the_writer_schema_are_passed_on_as_they_are(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        static::assertSame($rows, (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(int_schema(
            'id',
        )))))->conform($rows, new AdaptiveBackend()));
    }

    public function test_a_column_the_writer_does_not_know_is_passed_on_as_it_is(): void
    {
        $rows = array_to_rows([['id' => 1, 'extra' => 'x']], schema(int_schema('id'), str_schema('extra')));

        static::assertSame($rows, (new ParquetSchemaConformance((new SchemaConverter())->toParquet(schema(int_schema(
            'id',
        )))))->conform($rows, new AdaptiveBackend()));
    }
}
