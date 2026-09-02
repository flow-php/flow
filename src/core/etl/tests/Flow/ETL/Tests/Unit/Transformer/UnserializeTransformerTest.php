<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UnserializeTransformer;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class UnserializeTransformerTest extends FlowTestCase
{
    public function test_unserializing_row_from_entry(): void
    {
        $row1 = row(['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']]);
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $row2 = row(['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']]);

        $rows = rows(
            schema(str_schema('serialized')),
            row(['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer()),
                rows($rowSchema, $row1),
            )]),
            row(['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer()),
                rows($rowSchema, $row2),
            )]),
        );

        $transformer = new UnserializeTransformer('serialized', $rowSchema);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row1),
                    ),
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row2),
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
        $rows = rows(schema(str_schema('serialized')), row(['serialized' => 'not-serialized']));

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')));

        static::assertSame(
            [['serialized' => 'not-serialized', 'id' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_unserializing_row_without_source_column_emits_the_declared_shape(): void
    {
        $rows = rows(schema(int_schema('id')), row(['id' => 1]));

        $transformer = new UnserializeTransformer('serialized', schema(str_schema('name')));

        static::assertSame([['id' => 1, 'name' => null]], $transformer->transform($rows, flow_context())->toArray());
    }

    public function test_unserializing_non_string_value_emits_the_declared_shape(): void
    {
        $rows = rows(schema(int_schema('serialized')), row(['serialized' => 123]));

        $transformer = new UnserializeTransformer('serialized', schema(str_schema('name')));

        static::assertSame(
            [['serialized' => 123, 'name' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_unserializing_multi_row_payload_emits_the_declared_shape(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        $rows = rows(schema(str_schema('serialized')), row(['serialized' => $payload]));

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')));

        static::assertSame(
            [['serialized' => $payload, 'id' => null]],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }

    public function test_the_declared_columns_land_under_the_merge_prefix(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 7])),
        );

        $transformer = new UnserializeTransformer('serialized', schema(int_schema('id')), mergePrefix: 'payload_');

        static::assertSame(
            [['serialized' => $payload, 'payload_id' => 7]],
            $transformer
                ->transform(rows(schema(str_schema('serialized')), row(['serialized' => $payload])), flow_context())
                ->toArray(),
        );
    }

    public function test_unserializing_without_merge(): void
    {
        $row1 = row(['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']]);
        $rowSchema = schema(
            int_schema('id'),
            str_schema('name'),
            bool_schema('active'),
            list_schema('tags', type_list(type_string())),
        );
        $row2 = row(['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']]);

        $rows = rows(
            schema(str_schema('serialized')),
            row(['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer()),
                rows($rowSchema, $row1),
            )]),
            row(['serialized' => serialize_to_string(
                new Base64Serializer(new FloeSerializer()),
                rows($rowSchema, $row2),
            )]),
        );

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
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 7])),
        );

        static::assertSame(
            [['serialized' => $payload, 'id' => 7]],
            (new UnserializeTransformer('serialized', schema(int_schema('id'))))
                ->transform(
                    rows(
                        schema(str_schema('serialized'), int_schema('id')),
                        row(['serialized' => $payload, 'id' => 1]),
                    ),
                    flow_context(),
                )
                ->toArray(),
        );
    }

    public function test_a_declared_column_the_payload_does_not_carry_is_null(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 7])),
        );

        static::assertSame(
            [['serialized' => $payload, 'id' => 7, 'absent' => null]],
            (new UnserializeTransformer('serialized', schema(int_schema('id'), str_schema('absent'))))
                ->transform(rows(schema(str_schema('serialized')), row(['serialized' => $payload])), flow_context())
                ->toArray(),
        );
    }
}
