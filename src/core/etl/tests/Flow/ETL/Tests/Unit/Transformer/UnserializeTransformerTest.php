<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\UnserializeTransformer;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class UnserializeTransformerTest extends FlowTestCase
{
    public function test_unserializing_row_from_entry(): void
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
            row(str_entry('serialized', serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row1)))),
            row(str_entry('serialized', serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row2)))),
        );

        $transformer = new UnserializeTransformer('serialized');

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals(
            [
                [
                    'serialized' => serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row1)),
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'serialized' => serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row2)),
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
        $rows = rows(row(str_entry('serialized', 'not-serialized')));

        $transformer = new UnserializeTransformer('serialized');

        $transformedRows = $transformer->transform($rows, flow_context());

        static::assertEquals($rows, $transformedRows);
    }

    public function test_unserializing_row_without_source_column_is_unchanged(): void
    {
        $rows = rows(row(int_entry('id', 1)));

        $transformer = new UnserializeTransformer('serialized');

        static::assertEquals($rows, $transformer->transform($rows, flow_context()));
    }

    public function test_unserializing_non_string_value_is_unchanged(): void
    {
        $rows = rows(row(int_entry('serialized', 123)));

        $transformer = new UnserializeTransformer('serialized');

        static::assertEquals($rows, $transformer->transform($rows, flow_context()));
    }

    public function test_unserializing_multi_row_payload_returns_row_unchanged(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
        );
        $rows = rows(row(str_entry('serialized', $payload)));

        $transformer = new UnserializeTransformer('serialized');

        static::assertEquals($rows, $transformer->transform($rows, flow_context()));
    }

    public function test_unserializing_without_merge(): void
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
            row(str_entry('serialized', serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row1)))),
            row(str_entry('serialized', serialize_to_string(new Base64Serializer(new FloeSerializer()), rows($row2)))),
        );

        $transformer = new UnserializeTransformer('serialized', false);

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
}
