<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Integration;

use Flow\Bridge\Symfony\HttpFoundation\DataStream;
use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Flow\Bridge\Symfony\HttpFoundation\Output\JsonOutput;
use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\ETL\Config;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\StdOutFilesystem;

use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_on_complete;
use function Flow\Bridge\Symfony\HttpFoundation\http_xml_output;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\from_array;
use function json_decode;
use function usort;

final class FlowStreamedResponseTest extends FlowTestCase
{
    public function test_stream_closure_is_called_after_streaming_with_report(): void
    {
        $closureCalled = false;
        $receivedReport = null;

        $response = DataStream::open(from_array([
            ['id' => 1, 'name' => 'test'],
        ]))
            ->config(Config::builder()->analyze(analyze()))
            ->onComplete(http_on_complete(static function (?Report $report) use (
                &$closureCalled,
                &$receivedReport,
            ): void {
                $closureCalled = true;
                $receivedReport = $report;
            }))
            ->streamedResponse(new JsonOutput());

        $this->sendResponse($response);

        static::assertTrue($closureCalled);
        static::assertNotNull($receivedReport);
        static::assertSame(1, $receivedReport->statistics()->totalRows());
    }

    public function test_stream_closure_receives_null_without_analyze(): void
    {
        $receivedReport = 'not_null';

        $response = DataStream::open(from_array([
            ['id' => 1, 'name' => 'test'],
        ]))
            ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                $receivedReport = $report;
            }))
            ->streamedResponse(new JsonOutput());

        $this->sendResponse($response);

        static::assertNull($receivedReport);
    }

    public function test_stream_closure_receives_report_with_schema_when_enabled(): void
    {
        $receivedReport = null;

        $response = DataStream::open(from_array([
            ['id' => 1, 'name' => 'test'],
            ['id' => 2, 'name' => 'test2'],
        ]))
            ->config(Config::builder()->analyze(analyze()->withSchema()))
            ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                $receivedReport = $report;
            }))
            ->streamedResponse(new JsonOutput());

        $this->sendResponse($response);

        static::assertNotNull($receivedReport);
        static::assertSame(2, $receivedReport->statistics()->totalRows());
        $schema = $receivedReport->schema();
        static::assertNotNull($schema);
        static::assertCount(2, $schema->references()->all());
    }

    public function test_streamed_response_writes_through_the_stdout_filesystem(): void
    {
        // the response holds a StdOutFilesystem instance; the URI it builds must still resolve to it,
        // otherwise every streamed row throws on the first write
        $response = new FlowStreamedResponse(
            from_array([['id' => 1], ['id' => 2]]),
            new CSVOutput(),
            filesystem: new StdOutFilesystem(),
        );

        static::assertSame("id\n1\n2\n", $this->sendResponse($response));
    }

    public function test_streaming_array_response_to_csv(): void
    {
        $response = new FlowStreamedResponse(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]), new CSVOutput());

        static::assertEquals(<<<'CSV'
            id,size,color,ean
            1,XL,red,1234567890123
            2,M,blue,1234567890124
            3,S,green,1234567890125

            CSV, $this->sendResponse($response));
    }

    public function test_streaming_array_response_to_json(): void
    {
        $extractor = from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]);

        $response = DataStream::open($extractor)->streamedResponse(new JsonOutput());

        static::assertEquals(<<<'JSON'
            [{"id":1,"size":"XL","color":"red","ean":"1234567890123"},{"id":2,"size":"M","color":"blue","ean":"1234567890124"},{"id":3,"size":"S","color":"green","ean":"1234567890125"}]
            JSON, $this->sendResponse($response));
    }

    public function test_streaming_array_response_to_xml(): void
    {
        $response = new FlowStreamedResponse(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]), http_xml_output());

        static::assertEquals(<<<'XML'
            <?xml version="1.0" encoding="UTF-8"?>
            <rows>
            <row><id>1</id><size>XL</size><color>red</color><ean>1234567890123</ean></row>
            <row><id>2</id><size>M</size><color>blue</color><ean>1234567890124</ean></row>
            <row><id>3</id><size>S</size><color>green</color><ean>1234567890125</ean></row>
            </rows>
            XML, $this->sendResponse($response));
    }

    public function test_streaming_partitioned_dataset(): void
    {
        $response = new FlowStreamedResponse(
            from_json(__DIR__ . '/Fixtures/partitioned/**/*.json'),
            new JsonOutput(putRowsInNewLines: true),
        );

        /** @var list<array{id: int, color: string, size: string}> $rows */
        $rows = json_decode($this->sendResponse($response), true, flags: JSON_THROW_ON_ERROR);
        usort($rows, static fn(array $a, array $b): int => $a['id'] <=> $b['id']);

        static::assertSame(
            [
                ['id' => 1, 'color' => 'red', 'size' => 'small'],
                ['id' => 2, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 3, 'color' => 'green', 'size' => 'large'],
                ['id' => 4, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 5, 'color' => 'black', 'size' => 'medium'],
                ['id' => 6, 'color' => 'white', 'size' => 'large'],
                ['id' => 7, 'color' => 'red', 'size' => 'small'],
                ['id' => 8, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 9, 'color' => 'green', 'size' => 'large'],
                ['id' => 10, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 11, 'color' => 'black', 'size' => 'medium'],
                ['id' => 12, 'color' => 'white', 'size' => 'large'],
            ],
            $rows,
        );
    }

    public function test_streaming_with_disposition(): void
    {
        $response = DataStream::open(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]))->as('products.csv')->streamedResponse(http_csv_output());

        static::assertEquals('attachment; filename=products.csv', $response->headers->get('Content-Disposition'));
    }

    private function sendResponse(FlowStreamedResponse $response): string
    {
        ob_start();
        $response->send();

        return (string) ob_get_clean();
    }
}
