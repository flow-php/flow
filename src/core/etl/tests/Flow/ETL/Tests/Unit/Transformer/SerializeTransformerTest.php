<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{bool_entry, flow_context, int_entry, list_entry, row, rows, str_entry, type_list, type_string};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializeTransformer;
use Flow\Serializer\{Base64Serializer, NativePHPSerializer};

final class SerializeTransformerTest extends FlowTestCase
{
    public function test_serializing_empty_row_under_one_entry() : void
    {
        $rows = rows(
            $row1 = row(),
        );

        $transformer = new SerializeTransformer('serialized');
        $transformedRows = $transformer->transform($rows, flow_context());

        self::assertEquals(
            [
                [
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row1),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_one_entry() : void
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

        self::assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row1),
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row2),
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_serializing_row_under_standalone_entry() : void
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

        self::assertEquals(
            [
                [
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row1),
                ],
                [
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row2),
                ],
            ],
            $transformedRows->toArray(),
        );
    }
}
