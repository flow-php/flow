<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializeTransformer;
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

final class SerializeTransformerTest extends FlowTestCase
{
    public function test_bind_adds_the_target_column_when_the_input_does_not_declare_it(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('serialized')),
            (new SerializeTransformer('serialized'))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_bind_collapses_the_input_to_the_target_column_when_standalone(): void
    {
        static::assertEquals(
            schema(str_schema('serialized')),
            (new SerializeTransformer('serialized', standalone: true))->bind(schema(
                int_schema('id'),
                str_schema('name'),
            ))->output,
        );
    }

    public function test_bind_replaces_the_target_column_with_a_string_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('payload')),
            (new SerializeTransformer('payload'))->bind(schema(int_schema('id'), int_schema('payload')))->output,
        );
    }

    public function test_serializing_empty_row_under_one_entry(): void
    {
        $rows = Rows::fromColumns(schema(), [], 1);

        $transformer = new SerializeTransformer('serialized');
        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $rows,
                    ),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_one_entry(): void
    {
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $rows = array_to_rows([
            ['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']],
            ['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']],
        ], $rowSchema);

        $transformer = new SerializeTransformer('serialized');

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $rows->slice(0, 1),
                    ),
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $rows->slice(1, 1),
                    ),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_standalone_entry(): void
    {
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $rows = array_to_rows([
            ['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']],
            ['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']],
        ], $rowSchema);

        $transformer = new SerializeTransformer('serialized', true);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $rows->slice(0, 1),
                    ),
                ],
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
                        $rows->slice(1, 1),
                    ),
                ],
            ],
            $transformedRows->toArray(),
        );
    }
}
