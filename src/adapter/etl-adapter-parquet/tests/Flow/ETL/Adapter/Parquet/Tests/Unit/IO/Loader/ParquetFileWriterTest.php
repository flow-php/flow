<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Loader;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetFileWriter;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetSchemaConformance;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;

final class ParquetFileWriterTest extends FlowTestCase
{
    /**
     * @return iterable<string, array{Rows, list<array<array-key, mixed>>}>
     */
    public static function nested_and_typed_values(): iterable
    {
        $uuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';
        $at = new DateTimeImmutable('2024-01-01 12:00:00 UTC');

        yield 'datetime and scalar' => [
            array_to_rows([['id' => 1, 'at' => $at]], schema(int_schema('id'), datetime_schema('at'))),
            [['id' => 1, 'at' => $at]],
        ];
        yield 'nulls' => [
            array_to_rows(
                [['id' => null, 'name' => null]],
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            ),
            [['id' => null, 'name' => null]],
        ];
        yield 'uuid, null beside a value' => [
            array_to_rows(
                [['id' => new Uuid($uuid), 'n' => 1], ['id' => null, 'n' => 2]],
                schema(uuid_schema('id', nullable: true), int_schema('n')),
            ),
            [['id' => $uuid, 'n' => 1], ['id' => null, 'n' => 2]],
        ];
        yield 'json and uuid in a structure' => [
            array_to_rows([['body' => [
                'data' => Json::fromArray(['a' => 1]),
                'id' => new Uuid($uuid),
                'n' => 1,
            ]]], schema(structure_schema('body', type_structure([
                'data' => type_json(),
                'id' => type_uuid(),
                'n' => type_integer(),
            ])))),
            [['body' => ['data' => '{"a":1}', 'id' => $uuid, 'n' => 1]]],
        ];
        yield 'json in a deep structure' => [
            array_to_rows([[
                'outer' => ['inner' => ['deep' => Json::fromArray(['e' => 5])]],
            ]], schema(structure_schema('outer', type_structure(['inner' => type_structure(['deep' => type_json()])])))),
            [['outer' => ['inner' => ['deep' => '{"e":5}']]]],
        ];
        yield 'uuid list elements' => [
            array_to_rows([['ids' => [new Uuid($uuid)]]], schema(list_schema('ids', type_list(type_uuid())))),
            [['ids' => [$uuid]]],
        ];
        yield 'uuid map values keep their keys' => [
            array_to_rows([['ids' => [
                'a' => new Uuid($uuid),
            ]]], schema(map_schema('ids', type_map(type_string(), type_uuid())))),
            [['ids' => ['a' => $uuid]]],
        ];
        yield 'uuids in a list of structures' => [
            array_to_rows([[
                'items' => [['x' => new Uuid($uuid)]],
            ]], schema(list_schema('items', type_list(type_structure(['x' => type_uuid()]))))),
            [['items' => [['x' => $uuid]]]],
        ];
    }

    /**
     * @param list<array<array-key, mixed>> $expected
     */
    #[DataProvider('nested_and_typed_values')]
    public function test_every_value_is_written_as_the_file_stores_it(Rows $rows, array $expected): void
    {
        static::assertEquals($expected, ParquetFilesContext::writtenThroughTheLoader($rows));
    }

    public function test_written_batches_read_back_equal(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $parquetSchema = (new SchemaConverter())->toParquetSchema($schema);
        $writer = new ParquetFileWriter(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://out.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $schema));
        $writer->write(array_to_rows([['id' => 3, 'name' => 'c']], $schema));
        $writer->close();

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null], ['id' => 3, 'name' => 'c']],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_a_column_of_another_stored_type_is_conformed_before_it_is_written(): void
    {
        $filesystem = memory_filesystem();
        $parquetSchema = (new SchemaConverter())->toParquetSchema(schema(int_schema('id', nullable: true)));
        $writer = new ParquetFileWriter(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://out.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => '1'], ['id' => '2']], schema(str_schema('id'))));
        $writer->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }
}
