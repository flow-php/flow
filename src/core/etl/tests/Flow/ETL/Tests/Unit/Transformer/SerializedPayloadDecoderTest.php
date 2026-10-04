<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializedPayloadDecoder;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Serializer\DSL\serialize_to_string;

final class SerializedPayloadDecoderTest extends FlowTestCase
{
    public function test_a_multi_row_payload_gives_up(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));

        static::assertSame(
            [[]],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_rows([['serialized' => $payload]], schema(str_schema('serialized'))), 0)
                ->toArray(),
        );
    }

    public function test_a_non_string_value_gives_up(): void
    {
        static::assertSame(
            [[]],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_rows([['serialized' => 123]], schema(int_schema('serialized'))), 0)
                ->toArray(),
        );
    }

    public function test_a_payload_that_does_not_deserialize_gives_up(): void
    {
        static::assertSame(
            [[]],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_rows([['serialized' => 'not-serialized']], schema(str_schema('serialized'))), 0)
                ->toArray(),
        );
    }

    public function test_a_row_without_the_source_column_gives_up(): void
    {
        static::assertSame(
            [[]],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_rows([['other' => 1]], schema(int_schema('other'))), 0)
                ->toArray(),
        );
    }

    public function test_it_reads_the_declared_columns_out_of_the_payload(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer(new AdaptiveBackend())), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id'))));

        static::assertSame(
            [['id' => 7]],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_rows([
                    ['serialized' => 'not-serialized'],
                    ['serialized' => $payload],
                ], schema(str_schema('serialized'))), 1)
                ->toArray(),
        );
    }

    public static function decoder(): SerializedPayloadDecoder
    {
        return SerializedPayloadDecoder::of(
            ref('serialized'),
            new Base64Serializer(new FloeSerializer(new AdaptiveBackend())),
            ['id'],
        );
    }
}
