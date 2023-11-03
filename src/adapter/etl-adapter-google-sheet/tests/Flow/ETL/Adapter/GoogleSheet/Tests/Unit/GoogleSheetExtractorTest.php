<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\GoogleSheet;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use PHPUnit\Framework\TestCase;

final class GoogleSheetExtractorTest extends TestCase
{
    public function test_rows_in_batch_must_be_positive_integer() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Last row must be greater than 0');

        GoogleSheet::from_columns(
            $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            0
        );
    }

    public function test_stop_fetching_data_when_no_more_data_is_returned() : void
    {
        $extractor = GoogleSheet::from_columns(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            150,
        );

        $firstValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $firstValueRangeMock->method('getValues')->willReturn([
            ['header'],
            ['row1'],
            ...\array_fill(2, 49, ['row']),
        ]);
        $secondValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $secondValueRangeMock->method('getValues')->willReturn([]);
        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));

        $spreadsheetsValues->expects($this->exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($firstValueRangeMock, $secondValueRangeMock);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext(Config::default())));
        $this->assertCount(1, $rowsArray);
        $this->assertSame(50, $rowsArray[0]->count());
        $this->assertEquals(Row::create(Entry::string('header', 'row1')), $rowsArray[0]->first());
    }

    public function test_stop_fetching_data_when_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $extractor = GoogleSheet::from_columns(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            150,
        );

        $firstValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $firstValueRangeMock->method('getValues')->willReturn([
            ['header'],
            ['row1'],
            ...\array_fill(2, 99, ['row']),
        ]);
        $secondValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $secondValueRangeMock->method('getValues')->willReturn([
            ['row2'],
            ...\array_fill(2, 49, ['row']),
        ]);
        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));

        $spreadsheetsValues->expects($this->exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($firstValueRangeMock, $secondValueRangeMock);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext(Config::default())));
        $this->assertCount(2, $rowsArray);
        $this->assertSame(100, $rowsArray[0]->count());
        $this->assertEquals(Row::create(Entry::string('header', 'row1')), $rowsArray[0]->first());
        $this->assertSame(50, $rowsArray[1]->count());
        $this->assertEquals(Row::create(Entry::string('header', 'row2')), $rowsArray[1]->first());
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
        $valueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $valueRangeMock->method('getValues')->willReturn(null);

        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($valueRangeMock);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext(Config::default())));
        $this->assertCount(0, $rowsArray);
    }
}
