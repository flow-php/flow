<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\Sheet;

use Flow\ETL\Adapter\Excel\Sheet\OpenSheet;
use Flow\ETL\Adapter\Excel\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\Sheet\SheetsManager;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Extractor\Grid\RecordDecoder;
use Flow\ETL\Tests\FlowTestCase;
use OpenSpout\Reader\Exception\ReaderNotOpenedException;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

final class OpenSheetTest extends FlowTestCase
{
    public function test_it_carries_the_cells_and_the_decoder_of_one_handle(): void
    {
        $reader = new XlsxReader();
        $reader->open(ExcelFixtureContext::path('fixture.xlsx')->path());

        $open = new OpenSheet(
            $reader,
            (new SheetCells((new SheetsManager($reader->getSheetIterator()))->first(), true, 1))->rows(),
            $decoder = new RecordDecoder(withHeader: true, emptyToNull: true),
        );

        static::assertSame(['id', 'name', 'email'], $open->cells->current());
        static::assertSame($decoder, $open->decoder);

        $open->close();
    }

    public function test_closing_it_closes_the_reader_it_was_given(): void
    {
        $reader = new XlsxReader();
        $reader->open(ExcelFixtureContext::path('fixture.xlsx')->path());

        (new OpenSheet(
            $reader,
            (new SheetCells((new SheetsManager($reader->getSheetIterator()))->first(), true, 1))->rows(),
            new RecordDecoder(withHeader: true, emptyToNull: true),
        ))->close();

        $this->expectException(ReaderNotOpenedException::class);
        $this->expectExceptionMessage('Reader should be opened first.');

        $reader->getSheetIterator();
    }
}
