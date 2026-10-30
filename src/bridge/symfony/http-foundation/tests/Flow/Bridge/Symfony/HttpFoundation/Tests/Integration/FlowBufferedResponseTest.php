<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Integration;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowBufferedResponse;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class FlowBufferedResponseTest extends TestCase
{
    public function test_a_buffered_response_refuses_a_config_for_a_data_frame_source(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A DataFrame source carries its own config');

        new FlowBufferedResponse(df()->read(from_array([['id' => 1]])), http_csv_output(), config: config_builder());
    }

    public function test_a_data_frame_source_is_buffered_with_the_headers(): void
    {
        $response = http_stream_open(df()->read(from_array([['id' => 1], ['id' => 2]]))->limit(1))
            ->headers(['X-Total-Rows' => '2'])
            ->response(http_csv_output());

        static::assertSame("id\n1\n", $response->getContent());
        static::assertSame('2', $response->headers->get('X-Total-Rows'));
    }

    public function test_buffered_response_reads_back_what_it_wrote(): void
    {
        // identity, not protocol, makes a MemoryFilesystem a buffer: reading back through a different
        // instance returns nothing and the response silently degrades to HTTP 204
        $response = http_stream_open(from_array([['id' => 1], ['id' => 2]]))->response(http_csv_output());

        static::assertSame(200, $response->getStatusCode());
        static::assertSame("id\n1\n2\n", $response->getContent());
    }

    public function test_buffering_array_response_to_csv(): void
    {
        $response = http_stream_open(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]))
            ->response(http_csv_output());

        static::assertEquals(<<<'CSV'
            id,size,color,ean
            1,XL,red,1234567890123
            2,M,blue,1234567890124
            3,S,green,1234567890125

            CSV, $response->getContent());
    }

    public function test_buffering_array_response_to_json(): void
    {
        $response = http_stream_open(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]))
            ->response(http_json_output());

        static::assertEquals(<<<'JSON'
            [{"id":1,"size":"XL","color":"red","ean":"1234567890123"},{"id":2,"size":"M","color":"blue","ean":"1234567890124"},{"id":3,"size":"S","color":"green","ean":"1234567890125"}]
            JSON, $response->getContent());
    }
}
