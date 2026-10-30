<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Operators\Transformer\SerializeTransformer;
use Flow\ETL\Operators\Transformer\UnserializeTransformer;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;

use function base64_encode;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
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
        $payload = base64_encode(serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([[
            'id' => '12',
        ]], schema(str_schema('id')))));

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 0): could not convert \'12\' (string) to integer');

        (new UnserializeTransformer('serialized', schema(int_schema('id')), config()->serializer()))->transform(
            array_to_rows([[
                'serialized' => $payload,
            ]], schema(str_schema('serialized'))),
            flow_context()->backend(),
        );
    }

    public function test_bind_expands_the_declared_target_into_nullable_columns(): void
    {
        static::assertEquals(
            schema(str_schema('serialized'), int_schema('id', nullable: true), str_schema('name', nullable: true)),
            (new UnserializeTransformer(
                'serialized',
                schema(int_schema('id'), str_schema('name')),
                config()->serializer(),
            ))->bind(schema(str_schema('serialized')))->output,
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
                config()->serializer(),
                'row_',
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

        static::assertEquals(
            [
                [
                    'serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $source->slice(0, 1),
                    )),
                    'id' => 1,
                    'name' => 'John',
                    'active' => true,
                    'tags' => ['tag1', 'tag2'],
                ],
                [
                    'serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $source->slice(1, 1),
                    )),
                    'id' => 2,
                    'name' => 'Jane',
                    'active' => false,
                    'tags' => ['tag3', 'tag4'],
                ],
            ],
            (new UnserializeTransformer('serialized', $rowSchema, config()->serializer()))
                ->transform(array_to_rows([
                    ['serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $source->slice(0, 1),
                    ))],
                    ['serialized' => base64_encode(serialize_to_string(
                        new FloeSerializer(new AdaptiveBackend()),
                        $source->slice(1, 1),
                    ))],
                ], schema(str_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_unserializing_something_that_is_not_serialized_row(): void
    {
        static::assertSame(
            [['serialized' => 'not-serialized', 'id' => null]],
            (new UnserializeTransformer('serialized', schema(int_schema('id')), config()->serializer()))
                ->transform(array_to_rows([[
                    'serialized' => 'not-serialized',
                ]], schema(str_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_unserializing_row_without_source_column_emits_the_declared_shape(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => null]],
            (new UnserializeTransformer('serialized', schema(str_schema('name')), config()->serializer()))
                ->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_unserializing_non_string_value_emits_the_declared_shape(): void
    {
        static::assertSame(
            [['serialized' => 123, 'name' => null]],
            (new UnserializeTransformer('serialized', schema(str_schema('name')), config()->serializer()))
                ->transform(array_to_rows([[
                    'serialized' => 123,
                ]], schema(int_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_unserializing_multi_row_payload_emits_the_declared_shape(): void
    {
        $payload = base64_encode(serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id')))));
        static::assertSame(
            [['serialized' => $payload, 'id' => null]],
            (new UnserializeTransformer('serialized', schema(int_schema('id')), config()->serializer()))
                ->transform(array_to_rows([[
                    'serialized' => $payload,
                ]], schema(str_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_the_declared_columns_land_under_the_merge_prefix(): void
    {
        $payload = base64_encode(serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id')))));

        static::assertSame(
            [['serialized' => $payload, 'payload_id' => 7]],
            (new UnserializeTransformer(
                'serialized',
                schema(int_schema('id')),
                config()->serializer(),
                mergePrefix: 'payload_',
            ))
                ->transform(array_to_rows([[
                    'serialized' => $payload,
                ]], schema(str_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_a_declared_column_replaces_an_input_column_of_the_same_name(): void
    {
        $payload = base64_encode(serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id')))));

        static::assertSame(
            [['serialized' => $payload, 'id' => 7]],
            (new UnserializeTransformer('serialized', schema(int_schema('id')), config()->serializer()))
                ->transform(
                    array_to_rows(
                        [['serialized' => $payload, 'id' => 1]],
                        schema(str_schema('serialized'), int_schema('id')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_a_declared_column_the_payload_does_not_carry_is_null(): void
    {
        $payload = base64_encode(serialize_to_string(new FloeSerializer(new AdaptiveBackend()), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id')))));

        static::assertSame(
            [['serialized' => $payload, 'id' => 7, 'absent' => null]],
            (new UnserializeTransformer(
                'serialized',
                schema(int_schema('id'), str_schema('absent')),
                config()->serializer(),
            ))
                ->transform(array_to_rows([[
                    'serialized' => $payload,
                ]], schema(str_schema('serialized'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));
        $serialized = (new SerializeTransformer('payload', config()->serializer()))
            ->bind($schema)
            ->step->transform(
                array_to_rows(
                    [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
                    schema(int_schema('id'), str_schema('name')),
                ),
                flow_context()->backend(),
            );
        $transformer = new UnserializeTransformer('payload', $schema, config()->serializer(), 'u_');
        static::assertSame(
            StepRoundTrip::transformed($transformer, $serialized->schema(), $serialized, flow_context()),
            StepRoundTrip::transformed(
                DescribeRoundTrip::rebuilt($transformer),
                $serialized->schema(),
                $serialized,
                flow_context(),
            ),
        );
    }
}
