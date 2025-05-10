<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\Function;

use function Flow\ETL\Adapter\Excel\DSL\is_valid_excel_sheet_name;
use function Flow\ETL\DSL\{ref, row, str_entry};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class IsValidExcelSheetNameTest extends FlowTestCase
{
    #[TestWith([''])]
    #[TestWith(['This is veeeeeery long excel sheet name, longer than 32 characters'])]
    #[TestWith(['Sheet/Name'])]
    #[TestWith(['Sheet*Name'])]
    #[TestWith(['Sheet?Name'])]
    public function test_invalid_excel_sheet_name(string $invalidNames) : void
    {
        self::assertFalse(is_valid_excel_sheet_name($invalidNames)->eval(row()));
    }

    #[TestWith(['Sheet1'])]
    #[TestWith(['Excel Sheet'])]
    public function test_valid_excel_sheet_name(string $sheetName) : void
    {
        self::assertTrue(is_valid_excel_sheet_name(ref('sheet_name'))->eval(row(str_entry('sheet_name', $sheetName))));
    }
}
