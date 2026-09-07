<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Response;

use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class FlowBufferedResponseTest extends FlowTestCase
{
    public function test_response_from_empty_dataset(): void
    {
        $response = http_stream_open(from_array([]))->response(http_json_output());

        static::assertEquals('', $response->getContent());
        static::assertEquals(204, $response->getStatusCode());
    }

    public function test_response_is_buffered_only_once(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')), rows(schema(int_schema('id')), row(['id' => 1])));

        $response = http_stream_open($extractor)->response(http_json_output());

        $response->getContent();
        $response->getContent();

        static::assertSame(1, $extractor->extractCalls);
        static::assertEquals('[{"id":1}]', $response->getContent());
        static::assertEquals(200, $response->getStatusCode());
    }
}
