<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Integration;

use Flow\Bridge\Symfony\HttpFoundation\DataStream;
use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Flow\Bridge\Symfony\HttpFoundation\Output\JsonOutput;
use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\Bridge\Symfony\HttpFoundation\Tests\Context\ResponseContext;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\StdOutFilesystem;

use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_on_complete;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\Bridge\Symfony\HttpFoundation\http_xml_output;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function json_decode;
use function usort;

final class FlowStreamedResponseTest extends FlowTestCase
{
    public function test_a_data_frame_source_hands_the_report_of_its_own_config(): void
    {
        $receivedReport = null;

        ResponseContext::send(
            http_stream_open(df(config_builder()->analyze(analyze()))->read(from_array([['id' => 1], ['id' => 2]])))
                ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                    $receivedReport = $report;
                }))
                ->streamedResponse(new JsonOutput()),
        );

        static::assertNotNull($receivedReport);
        static::assertSame(2, $receivedReport->statistics()->totalRows());
    }

    public function test_a_data_frame_source_streams_its_rows_with_the_headers(): void
    {
        $response = http_stream_open(df()->read(from_array([['id' => 1], ['id' => 2]]))->limit(1))
            ->headers(['X-Total-Rows' => '2'])
            ->streamedResponse(new CSVOutput());

        static::assertSame("id\n1\n", ResponseContext::send($response));
        static::assertSame('2', $response->headers->get('X-Total-Rows'));
    }

    public function test_a_data_frame_source_without_analyze_hands_null(): void
    {
        $receivedReport = 'not_null';

        ResponseContext::send(
            http_stream_open(df()->read(from_array([['id' => 1]])))
                ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                    $receivedReport = $report;
                }))
                ->streamedResponse(new JsonOutput()),
        );

        static::assertNull($receivedReport);
    }

    public function test_a_streamed_response_refuses_a_config_for_a_data_frame_source(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A DataFrame source carries its own config');

        new FlowStreamedResponse(df()->read(from_array([['id' => 1]])), new CSVOutput(), config: config_builder());
    }

    public function test_stream_closure_is_called_after_streaming_with_report(): void
    {
        $closureCalled = false;
        $receivedReport = null;

        ResponseContext::send(
            DataStream::open(from_array([
                ['id' => 1, 'name' => 'test'],
            ]))
                ->config(config_builder()->analyze(analyze()))
                ->onComplete(http_on_complete(static function (?Report $report) use (
                    &$closureCalled,
                    &$receivedReport,
                ): void {
                    $closureCalled = true;
                    $receivedReport = $report;
                }))
                ->streamedResponse(new JsonOutput()),
        );

        static::assertTrue($closureCalled);
        static::assertNotNull($receivedReport);
        static::assertSame(1, $receivedReport->statistics()->totalRows());
    }

    public function test_stream_closure_receives_null_without_analyze(): void
    {
        $receivedReport = 'not_null';

        ResponseContext::send(
            DataStream::open(from_array([
                ['id' => 1, 'name' => 'test'],
            ]))
                ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                    $receivedReport = $report;
                }))
                ->streamedResponse(new JsonOutput()),
        );

        static::assertNull($receivedReport);
    }

    public function test_stream_closure_receives_report_with_schema_when_enabled(): void
    {
        $receivedReport = null;

        ResponseContext::send(
            DataStream::open(from_array([
                ['id' => 1, 'name' => 'test'],
                ['id' => 2, 'name' => 'test2'],
            ]))
                ->config(config_builder()->analyze(analyze()->withSchema()))
                ->onComplete(http_on_complete(static function (?Report $report) use (&$receivedReport): void {
                    $receivedReport = $report;
                }))
                ->streamedResponse(new JsonOutput()),
        );

        static::assertNotNull($receivedReport);
        static::assertSame(2, $receivedReport->statistics()->totalRows());
        $schema = $receivedReport->schema();
        static::assertNotNull($schema);
        static::assertCount(2, $schema->names());
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

        static::assertSame("id\n1\n2\n", ResponseContext::send($response));
    }

    public function test_streaming_array_response_to_csv(): void
    {
        static::assertEquals(<<<'CSV'
            id,size,color,ean
            1,XL,red,1234567890123
            2,M,blue,1234567890124
            3,S,green,1234567890125

            CSV, ResponseContext::send(
            new FlowStreamedResponse(from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]), new CSVOutput()),
        ));
    }

    public function test_streaming_array_response_to_json(): void
    {
        static::assertEquals(<<<'JSON'
            [{"id":1,"size":"XL","color":"red","ean":"1234567890123"},{"id":2,"size":"M","color":"blue","ean":"1234567890124"},{"id":3,"size":"S","color":"green","ean":"1234567890125"}]
            JSON, ResponseContext::send(DataStream::open(from_array([
            ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
            ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
            ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
        ]))->streamedResponse(new JsonOutput())));
    }

    public function test_streaming_array_response_to_xml(): void
    {
        static::assertEquals(<<<'XML'
            <?xml version="1.0" encoding="UTF-8"?>
            <rows>
            <row><id>1</id><size>XL</size><color>red</color><ean>1234567890123</ean></row>
            <row><id>2</id><size>M</size><color>blue</color><ean>1234567890124</ean></row>
            <row><id>3</id><size>S</size><color>green</color><ean>1234567890125</ean></row>
            </rows>
            XML, ResponseContext::send(
            new FlowStreamedResponse(from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]), http_xml_output()),
        ));
    }

    public function test_streaming_partitioned_dataset(): void
    {
        /** @var list<array{id: int, color: string, size: string}> $rows */
        $rows = json_decode(
            ResponseContext::send(
                new FlowStreamedResponse(
                    from_json(__DIR__ . '/Fixtures/partitioned/**/*.json'),
                    new JsonOutput(putRowsInNewLines: true),
                ),
            ),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
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
        static::assertEquals(
            'attachment; filename=products.csv',
            DataStream::open(from_array([
                ['id' => 1, 'size' => 'XL', 'color' => 'red', 'ean' => '1234567890123'],
                ['id' => 2, 'size' => 'M', 'color' => 'blue', 'ean' => '1234567890124'],
                ['id' => 3, 'size' => 'S', 'color' => 'green', 'ean' => '1234567890125'],
            ]))
                ->as('products.csv')
                ->streamedResponse(http_csv_output())
                ->headers->get('Content-Disposition'),
        );
    }
}
