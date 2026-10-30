<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Operators\Transformer\SerializeTransformer;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Rows;

use function base64_encode;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
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
            (new SerializeTransformer('serialized', config()->serializer()))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_bind_replaces_the_target_column_with_a_string_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('payload')),
            (new SerializeTransformer('payload', config()->serializer()))->bind(schema(
                int_schema('id'),
                int_schema('payload'),
            ))->output,
        );
    }

    public function test_serializing_empty_row_under_one_entry(): void
    {
        $rows = Rows::fromColumns(schema(), [], 1);

        static::assertEquals(
            [
                [
                    'serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $rows,
                    )),
                ],
            ],
            (new SerializeTransformer('serialized', config()->serializer()))
                ->transform($rows, flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_serializing_row_under_one_entry(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'name' => 'John', 'active' => true, 'tags' => ['tag1', 'tag2']],
                ['id' => 2, 'name' => 'Jane', 'active' => false, 'tags' => ['tag3', 'tag4']],
            ],
            schema(
                int_schema('id'),
                str_schema('name'),
                bool_schema('active'),
                list_schema('tags', type_list(type_string())),
            ),
        );

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                    'serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $rows->slice(0, 1),
                    )),
                ],
                [
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                    'serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $rows->slice(1, 1),
                    )),
                ],
            ],
            (new SerializeTransformer('serialized', config()->serializer()))
                ->transform($rows, flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new SerializeTransformer(ref('payload'), config()->serializer());
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }
}
