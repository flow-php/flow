<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Unit\Response;

use PHPUnit\Framework\TestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\DSL\from_array;

final class FlowStreamedResponseTest extends TestCase
{
    public function test_response_from_empty_dataset(): void
    {
        $response = http_stream_open(from_array([]))->streamedResponse(http_json_output());

        static::assertEquals('', $response->getContent());
        static::assertEquals(200, $response->getStatusCode());
    }
}
