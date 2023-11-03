<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Config;
use Flow\ETL\DSL\GoogleSheet;
use Flow\ETL\FlowContext;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use PHPUnit\Framework\TestCase;

final class GoogleSheetExtractorTest extends TestCase
{
    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $extractor = GoogleSheet::from_columns(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
        );

        $valueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $valueRangeMock->method('getValues')->willReturn([
            ['header'],
            ['row1'],
            ['row2'],
        ]);
        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));

        $spreadsheetsValues->expects($this->once())
            ->method('get')
            ->willReturn($valueRangeMock);

        $this->assertSame(
            [
                [
                    'header' => 'row1',
                ],
                [
                    'header' => 'row2',
                ],
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))[0]->toArray()
        );
    }

    public function test_works_for_no_data() : void
    {
        $extractor = GoogleSheet::from_columns(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            20
        );
        $ValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $ValueRangeMock->method('getValues')->willReturn(null);

        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($ValueRangeMock);

        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext(Config::default())))[0]->toArray();
        $this->assertCount(0, $rowsArray);
    }
}
