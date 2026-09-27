<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializedPayloadDecoder;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\array_to_row;
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
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer()), array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));

        static::assertSame(
            [],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_row(['serialized' => $payload], schema(str_schema('serialized'))))
                ->values(),
        );
    }

    public function test_a_non_string_value_gives_up(): void
    {
        static::assertSame(
            [],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_row(['serialized' => 123], schema(int_schema('serialized'))))
                ->values(),
        );
    }

    public function test_a_payload_that_does_not_deserialize_gives_up(): void
    {
        static::assertSame(
            [],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_row(['serialized' => 'not-serialized'], schema(str_schema('serialized'))))
                ->values(),
        );
    }

    public function test_a_row_without_the_source_column_gives_up(): void
    {
        static::assertSame(
            [],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_row(['other' => 1], schema(int_schema('other'))))
                ->values(),
        );
    }

    public function test_it_reads_the_declared_columns_out_of_the_payload(): void
    {
        $payload = serialize_to_string(new Base64Serializer(new FloeSerializer()), array_to_rows([[
            'id' => 7,
        ]], schema(int_schema('id'))));

        static::assertSame(
            ['id' => 7],
            SerializedPayloadDecoderTest::decoder()
                ->decode(array_to_row(['serialized' => $payload], schema(str_schema('serialized'))))
                ->values(),
        );
    }

    public static function decoder(): SerializedPayloadDecoder
    {
        return SerializedPayloadDecoder::of(ref('serialized'), new Base64Serializer(new FloeSerializer()), ['id']);
    }
}
