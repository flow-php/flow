<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UnserializeTransformer;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class UnserializeTransformerTest extends FlowTestCase
{
    public function test_a_payload_written_with_another_type_is_refused(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([[
            'id' => '12',
        ]], schema(str_schema('id'))));

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert \'12\' (string) to integer');

        (new UnserializeTransformer('serialized', schema(int_schema('id'))))->transform(array_to_rows([[
            'serialized' => $payload,
        ]], schema(str_schema('serialized'))), flow_context());
    }

    public function test_bind_expands_the_declared_target_into_nullable_columns(): void
    {
        static::assertEquals(
            schema(str_schema('serialized'), int_schema('id', nullable: true), str_schema('name', nullable: true)),
            (new UnserializeTransformer('serialized', schema(
                int_schema('id'),
                str_schema('name'),
            )))->bind(schema(str_schema('serialized')))->output,
        );
    }

    public function test_bind_prefixes_the_expanded_columns_with_the_merge_prefix(): void
    {
        static::assertEquals(
            schema(
                str_schema('serialized'),
                int_schema('row_id', nullable: true),
                str_schema('row_name', nullable: true),
            ),
            (new UnserializeTransformer(
                'serialized',
                schema(int_schema('id'), str_schema('name')),
                true,
                'row_',
            ))->bind(schema(str_schema('serialized')))->output,
        );
    }

    public function test_bind_without_merging_keeps_only_the_declared_target(): void
    {
        static::assertEquals(
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            (new UnserializeTransformer(
                'serialized',
                schema(int_schema('id'), str_schema('name')),
                false,
            ))->bind(schema(str_schema('serialized')))->output,
        );
    }

    public function test_unserializing_row_from_entry(): void
    {
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $source = array_to_rows([
            ['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']],
            ['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']],
        ], $rowSchema);

        $rows = array_to_rows([
            ['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                $source->slice(0, 1),
            )],
            ['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                $source->slice(1, 1),
            )],
        ], schema(str_schema('serialized')));

        $transformer = new UnserializeTransformer('serialized', $rowSchema);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $source->slice(0, 1),
                    ),
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $source->slice(1, 1),
                    ),
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_unserializing_something_that_is_not_serialized_row(): void
    {
        $rows = array_to_rows([['serialized' => 'not-serialized']], schema(str_schema('serialized')));

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')));

        static::assertSame(
            [['serialized' => 'not-serialized', 'id' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_unserializing_row_without_source_column_emits_the_declared_shape(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        $transformer = new UnserializeTransformer('serialized', schema(str_schema('name')));

        static::assertSame([['id' => 1, 'name' => null]], $transformer->transform($rows, flow_context())->toArray());
    }

    public function test_unserializing_non_string_value_emits_the_declared_shape(): void
    {
        $rows = array_to_rows([['serialized' => 123]], schema(int_schema('serialized')));

        $transformer = new UnserializeTransformer('serialized', schema(str_schema('name')));

        static::assertSame(
            [['serialized' => 123, 'name' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_unserializing_multi_row_payload_emits_the_declared_shape(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));
        $rows = array_to_rows([['serialized' => $payload]], schema(str_schema('serialized')));

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')));

        static::assertSame(
            [['serialized' => $payload, 'id' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_the_declared_columns_land_under_the_merge_prefix(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id'))));

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')), mergePrefix: 'payload_');

        static::assertSame(
            [['serialized' => $payload, 'payload_id' => 7]],
            $transformer
                ->transform(array_to_rows([[
                    'serialized' => $payload,
                ]], schema(str_schema('serialized'))), flow_context())
                ->toArray(),
        );
    }

    public function test_unserializing_without_merge(): void
    {
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $source = array_to_rows([
            ['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']],
            ['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']],
        ], $rowSchema);

        $rows = array_to_rows([
            ['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                $source->slice(0, 1),
            )],
            ['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                $source->slice(1, 1),
            )],
        ], schema(str_schema('serialized')));

        $transformer = new UnserializeTransformer('serialized', $rowSchema, false);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_a_declared_column_replaces_an_input_column_of_the_same_name(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id'))));

        static::assertSame(
            [['serialized' => $payload, 'id' => 7]],
            (new UnserializeTransformer('serialized', schema(int_schema('id'))))
                ->transform(
                    array_to_rows(
                        [['serialized' => $payload, 'id' => 1]],
                        schema(str_schema('serialized'), int_schema('id')),
                    ),
                    flow_context(),
                )
                ->toArray(),
        );
    }

    public function test_a_declared_column_the_payload_does_not_carry_is_null(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id'))));

        static::assertSame(
            [['serialized' => $payload, 'id' => 7, 'absent' => null]],
            (new UnserializeTransformer('serialized', schema(int_schema('id'), str_schema('absent'))))
                ->transform(array_to_rows([[
                    'serialized' => $payload,
                ]], schema(str_schema('serialized'))), flow_context())
                ->toArray(),
        );
    }
}
