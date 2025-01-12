<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{bool_entry, flow_context, int_entry, list_entry, row, rows, str_entry, type_list, type_string};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UnserializeTransformer;
use Flow\Serializer\{Base64Serializer, NativePHPSerializer};

final class UnserializeTransformerTest extends FlowTestCase
{
    public function test_unserializing_row_from_entry() : void
    {
        $row1 = row(
            int_entry('id', 1),
            str_entry('name', 'John'),
            bool_entry('active', true),
            list_entry('tags', ['tag1', 'tag2'], type_list(type_string())),
        );
        $row2 = row(
            int_entry('id', 2),
            str_entry('name', 'Jane'),
            bool_entry('active', false),
            list_entry('tags', ['tag3', 'tag4'], type_list(type_string())),
        );

        $rows = rows(
            row(str_entry('serialized', (new Base64Serializer(new NativePHPSerializer()))->serialize($row1))),
            row(str_entry('serialized', (new Base64Serializer(new NativePHPSerializer()))->serialize($row2))),
        );

        $transformer = new UnserializeTransformer('serialized');

        $transformedRows = $transformer->transform($rows, flow_context());

        self::assertEquals(
            [
                [
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row1),
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'serialized' => (new Base64Serializer(new NativePHPSerializer()))->serialize($row2),
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                ],
            ],
            $transformedRows->toArray(),
        );
    }

    public function test_unserializing_something_that_is_not_serialized_row() : void
    {
        $rows = rows(
            row(str_entry('serialized', 'not-serialized')),
        );

        $transformer = new UnserializeTransformer('serialized');

        $transformedRows = $transformer->transform($rows, flow_context());

        self::assertEquals($rows, $transformedRows);
    }

    public function test_unserializing_without_merge() : void
    {
        $row1 = row(
            int_entry('id', 1),
            str_entry('name', 'John'),
            bool_entry('active', true),
            list_entry('tags', ['tag1', 'tag2'], type_list(type_string())),
        );
        $row2 = row(
            int_entry('id', 2),
            str_entry('name', 'Jane'),
            bool_entry('active', false),
            list_entry('tags', ['tag3', 'tag4'], type_list(type_string())),
        );

        $rows = rows(
            row(str_entry('serialized', (new Base64Serializer(new NativePHPSerializer()))->serialize($row1))),
            row(str_entry('serialized', (new Base64Serializer(new NativePHPSerializer()))->serialize($row2))),
        );

        $transformer = new UnserializeTransformer('serialized', false);

        $transformedRows = $transformer->transform($rows, flow_context());

        self::assertEquals(
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
}
