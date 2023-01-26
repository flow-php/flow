<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use PHPUnit\Framework\TestCase;

final class GoogleSheetExtractorTest extends TestCase
{
    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $extractor = new GoogleSheetExtractor(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            GoogleSheetRange::create('sheet', 'A', 1, 'B', 10),
            true,
            20
        );
        $ValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $ValueRangeMock->method('getValues')->willReturn([
            ['header'],
            ['row1'],
            ['row2'],
        ]);
        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($ValueRangeMock);
        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->build())));
        $this->assertCount(1, $rowsArray);
        $this->assertSame(2, $rowsArray[0]->count());
    }

    public function test_works_for_no_data() : void
    {
        $extractor = new GoogleSheetExtractor(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            GoogleSheetRange::create('sheet', 'A', 1, 'B', 10),
            true,
            20
        );
        $ValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $ValueRangeMock->method('getValues')->willReturn([]);

        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($ValueRangeMock);
        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->build())));
        $this->assertCount(0, $rowsArray);
    }
}
