<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Integration;

use function Flow\Bridge\Symfony\HttpFoundation\{http_csv_output, http_json_output, http_stream_open};
use function Flow\ETL\DSL\from_array;
use PHPUnit\Framework\TestCase;

final class FlowBufferedResponseTest extends TestCase
{
    public function test_buffering_array_response_to_csv() : void
    {
        $response = http_stream_open(
            from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]),
        )->response(http_csv_output());

        self::assertEquals(<<<'CSV'
id,size,color,ean
1,XL,red,1234567890123
2,M,blue,1234567890124
3,S,green,1234567890125

CSV
            , $response->getContent());
    }

    public function test_buffering_array_response_to_json() : void
    {
        $response = http_stream_open(
            from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]),
        )->response(http_json_output());

        self::assertEquals(<<<'JSON'
[{"id":1,"size":"XL","color":"red","ean":"1234567890123"},{"id":2,"size":"M","color":"blue","ean":"1234567890124"},{"id":3,"size":"S","color":"green","ean":"1234567890125"}]
JSON
            , $response->getContent());
    }
}
