<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\Sheet;

use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Tests\FlowTestCase;

final class SheetCellsTest extends FlowTestCase
{
    public function test_an_xlsx_short_row_keeps_the_empty_cell_the_reader_gave_it(): void
    {
        $rows = ExcelFixtureContext::openSheetCells('fixture.xlsx', true, 1)->readAll();

        static::assertSame(['id', 'name', 'email'], $rows[0]);
        static::assertCount(3, $rows[10]);
        static::assertSame('', $rows[10][2]);
    }

    public function test_widens_a_short_ods_row_to_the_previous_width(): void
    {
        // the ODS reader drops trailing empty cells where the XLSX reader returns them as ''
        $rows = ExcelFixtureContext::openSheetCells('fixture.ods', true, 1)->readAll();

        static::assertSame(['id', 'name', 'email'], $rows[0]);
        static::assertCount(3, $rows[10]);
        static::assertNull($rows[10][2]);
    }
}
