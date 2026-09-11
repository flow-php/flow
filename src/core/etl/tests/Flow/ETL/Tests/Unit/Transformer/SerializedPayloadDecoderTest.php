<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SerializedPayloadDecoder;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Base64Serializer;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Serializer\DSL\serialize_to_string;

final class SerializedPayloadDecoderTest extends FlowTestCase
{
    public function test_a_multi_row_payload_gives_up(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );

        static::assertSame(
            ['id' => null],
            SerializedPayloadDecoderTest::decoder()->decode(row(['serialized' => $payload])),
        );
    }

    public function test_a_non_string_value_gives_up(): void
    {
        static::assertSame(['id' => null], SerializedPayloadDecoderTest::decoder()->decode(row(['serialized' => 123])));
    }

    public function test_a_payload_that_does_not_deserialize_gives_up(): void
    {
        static::assertSame(
            ['id' => null],
            SerializedPayloadDecoderTest::decoder()->decode(row(['serialized' => 'not-serialized'])),
        );
    }

    public function test_a_row_without_the_source_column_gives_up(): void
    {
        static::assertSame(['id' => null], SerializedPayloadDecoderTest::decoder()->decode(row(['other' => 1])));
    }

    public function test_it_reads_the_declared_columns_out_of_the_payload(): void
    {
        $payload = serialize_to_string(
            new Base64Serializer(new FloeSerializer()),
            rows(schema(int_schema('id')), row(['id' => 7])),
        );

        static::assertSame(
            ['id' => 7],
            SerializedPayloadDecoderTest::decoder()->decode(row(['serialized' => $payload])),
        );
    }

    public static function decoder(): SerializedPayloadDecoder
    {
        return SerializedPayloadDecoder::of(ref('serialized'), new Base64Serializer(new FloeSerializer()), [
            'id' => 'id',
        ]);
    }
}
