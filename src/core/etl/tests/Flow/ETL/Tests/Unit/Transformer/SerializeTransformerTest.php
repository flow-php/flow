<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializeTransformer;
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

final class SerializeTransformerTest extends FlowTestCase
{
    public function test_serializing_empty_row_under_one_entry(): void
    {
        $rows = rows($rowSchema = schema(), $row1 = row([]));

        $transformer = new SerializeTransformer('serialized');
        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row1),
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
        $rows = rows(
            $rowSchema,
            $row1 = row(['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']]),
            $row2 = row(['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']]),
        );

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
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row1),
                    ),
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row2),
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
        $rows = rows(
            $rowSchema,
            $row1 = row(['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']]),
            $row2 = row(['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']]),
        );

        $transformer = new SerializeTransformer('serialized', true);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row1),
                    ),
                ],
                [
                    'serialized' => serialize_to_string(
                        new Base64Serializer(new FloeSerializer()),
                        rows($rowSchema, $row2),
                    ),
                ],
            ],
            $transformedRows->toArray(),
        );
    }
}
