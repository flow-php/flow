<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Integration;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\from_array;
use Flow\Bridge\Symfony\HttpFoundation\{DataStream,
    FlowStreamedResponse,
    Output\CSVOutput,
    Output\JsonOutput,
    Output\XMLOutput};
use Flow\ETL\Tests\FlowTestCase;

final class FlowStreamedResponseTest extends FlowTestCase
{
    public function test_streaming_array_response_to_csv() : void
    {
        $response = new FlowStreamedResponse(
            from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]),
            new CSVOutput()
        );

        self::assertEquals(<<<'CSV'
id,size,color,ean
1,XL,red,1234567890123
2,M,blue,1234567890124
3,S,green,1234567890125

CSV
            , $this->sendResponse($response));
    }

    public function test_streaming_array_response_to_json() : void
    {
        $extractor = from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]);

        $response = DataStream::open($extractor)
            ->sendTo(new JsonOutput());

        self::assertEquals(<<<'JSON'
[{"id":1,"size":"XL","color":"red","ean":"1234567890123"},{"id":2,"size":"M","color":"blue","ean":"1234567890124"},{"id":3,"size":"S","color":"green","ean":"1234567890125"}]
JSON
            , $this->sendResponse($response));
    }

    public function test_streaming_array_response_to_xml() : void
    {
        $response = new FlowStreamedResponse(
            from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]),
            new XMLOutput()
        );

        self::assertEquals(<<<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<rows>
<row><id>1</id><size>XL</size><color>red</color><ean>1234567890123</ean></row>
<row><id>2</id><size>M</size><color>blue</color><ean>1234567890124</ean></row>
<row><id>3</id><size>S</size><color>green</color><ean>1234567890125</ean></row>
</rows>
XML
            , $this->sendResponse($response));
    }

    public function test_streaming_partitioned_dataset() : void
    {
        $response = new FlowStreamedResponse(
            from_json(__DIR__ . '/Fixtures/partitioned/**/*.json'),
            new JsonOutput(putRowsInNewLines: true)
        );

        self::assertEquals(<<<'JSON'
[
{"id":3,"color":"green","size":"large"},
{"id":9,"color":"green","size":"large"},
{"id":6,"color":"white","size":"large"},
{"id":12,"color":"white","size":"large"},
{"id":5,"color":"black","size":"medium"},
{"id":11,"color":"black","size":"medium"},
{"id":2,"color":"blue","size":"medium"},
{"id":8,"color":"blue","size":"medium"},
{"id":1,"color":"red","size":"small"},
{"id":7,"color":"red","size":"small"},
{"id":4,"color":"yellow","size":"small"},
{"id":10,"color":"yellow","size":"small"}
]
JSON
            , $this->sendResponse($response));
    }

    public function test_streaming_with_disposition() : void
    {
        $response = DataStream::open(
            from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ])
        )
            ->underFilename('products.csv')
            ->sendTo(new CSVOutput());

        self::assertEquals('attachment; filename=products.csv', $response->headers->get('Content-Disposition'));
    }

    private function sendResponse(FlowStreamedResponse $response) : string
    {
        ob_start();
        $response->send();

        return (string) ob_get_clean();
    }
}
