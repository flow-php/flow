<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializeTransformer;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class SerializeTransformerTest extends FlowTestCase
{
    public function test_serializing_empty_row_under_one_entry(): void
    {
        $rows = rows($row1 = row());

        $transformer = new SerializeTransformer('serialized');
        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => (new Base64Serializer(new FloeSerializer()))->serialize($row1),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_one_entry(): void
    {
        $rows = rows(
            $row1 = row(
                int_entry('id', 1),
                str_entry('name', 'John'),
                bool_entry('active', true),
                list_entry('tags', ['tag1', 'tag2'], type_list(type_string())),
            ),
            $row2 = row(
                int_entry('id', 2),
                str_entry('name', 'Jane'),
                bool_entry('active', false),
                list_entry('tags', ['tag3', 'tag4'], type_list(type_string())),
            ),
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
                    'serialized' => (new Base64Serializer(new FloeSerializer()))->serialize($row1),
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                    'serialized' => (new Base64Serializer(new FloeSerializer()))->serialize($row2),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_standalone_entry(): void
    {
        $rows = rows(
            $row1 = row(
                int_entry('id', 1),
                str_entry('name', 'John'),
                bool_entry('active', true),
                list_entry('tags', ['tag1', 'tag2'], type_list(type_string())),
            ),
            $row2 = row(
                int_entry('id', 2),
                str_entry('name', 'Jane'),
                bool_entry('active', false),
                list_entry('tags', ['tag3', 'tag4'], type_list(type_string())),
            ),
        );

        $transformer = new SerializeTransformer('serialized', true);

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => (new Base64Serializer(new FloeSerializer()))->serialize($row1),
                ],
                [
                    'serialized' => (new Base64Serializer(new FloeSerializer()))->serialize($row2),
                ],
            ],
            $transformedRows->toArray(),
        );
    }
}
