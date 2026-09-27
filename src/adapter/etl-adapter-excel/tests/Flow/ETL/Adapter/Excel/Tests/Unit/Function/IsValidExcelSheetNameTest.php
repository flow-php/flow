<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\Adapter\Excel\DSL\is_valid_excel_sheet_name;
use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IsValidExcelSheetNameTest extends FlowTestCase
{
    #[TestWith([''])]
    #[TestWith(['This is veeeeeery long excel sheet name, longer than 32 characters'])]
    #[TestWith(['Sheet/Name'])]
    #[TestWith(['Sheet*Name'])]
    #[TestWith(['Sheet?Name'])]
    public function test_invalid_excel_sheet_name(string $invalidNames): void
    {
        static::assertFalse(is_valid_excel_sheet_name($invalidNames)->eval(array_to_row([], schema()), flow_context()));
    }

    #[TestWith(['Sheet1'])]
    #[TestWith(['Excel Sheet'])]
    public function test_valid_excel_sheet_name(string $sheetName): void
    {
        static::assertTrue(
            is_valid_excel_sheet_name(ref('sheet_name'))
                ->eval(array_to_row(['sheet_name' => $sheetName], schema(str_schema('sheet_name'))), flow_context()),
        );
    }
}
