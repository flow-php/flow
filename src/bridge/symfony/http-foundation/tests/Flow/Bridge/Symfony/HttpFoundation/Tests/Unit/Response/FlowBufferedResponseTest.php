<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Response;

use function Flow\Bridge\Symfony\HttpFoundation\{http_json_output, http_stream_open};
use function Flow\ETL\DSL\{from_array, int_entry, row, rows};
use Flow\ETL\Extractor;
use Flow\ETL\Tests\FlowTestCase;

final class FlowBufferedResponseTest extends FlowTestCase
{
    public function test_response_from_empty_dataset() : void
    {
        $response = http_stream_open(from_array([]))->response(http_json_output());

        self::assertEquals('', $response->getContent());
        self::assertEquals(204, $response->getStatusCode());
    }

    public function test_response_is_buffered_only_once() : void
    {
        $extractor = $this->createMock(Extractor::class);

        $extractor->expects(self::once())->method('extract')->willReturn((function () : \Generator {
            yield rows(row(int_entry('id', 1)));
        })());

        $response = http_stream_open($extractor)->response(http_json_output());

        $response->getContent();
        $response->getContent();

        self::assertEquals('[{"id":1}]', $response->getContent());
        self::assertEquals(200, $response->getStatusCode());
    }
}
