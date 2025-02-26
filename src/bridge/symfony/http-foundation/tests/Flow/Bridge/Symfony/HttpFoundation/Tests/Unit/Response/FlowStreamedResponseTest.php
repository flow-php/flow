<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Response;

use function Flow\Bridge\Symfony\HttpFoundation\{http_json_output, http_stream_open};
use function Flow\ETL\DSL\from_array;
use PHPUnit\Framework\TestCase;

final class FlowStreamedResponseTest extends TestCase
{
    public function test_response_from_empty_dataset() : void
    {
        $response = http_stream_open(from_array([]))->streamedResponse(http_json_output());

        self::assertEquals('', $response->getContent());
        self::assertEquals(200, $response->getStatusCode());
    }
}
