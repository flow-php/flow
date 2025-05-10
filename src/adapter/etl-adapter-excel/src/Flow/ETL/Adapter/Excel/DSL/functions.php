<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\DSL;

use Flow\ETL\{Adapter\Excel\ExcelExtractor,
    Adapter\Excel\Function\IsValidExcelSheetName,
    Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType,
    Function\ScalarFunction};
use Flow\Filesystem\{Path};

#[DocumentationDSL(module: Module::EXCEL, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'excel')]
function from_excel(
    string|Path $path,
) : ExcelExtractor {
    return new ExcelExtractor(\is_string($path) ? Path::realpath($path) : $path);
}

function is_valid_excel_sheet_name(string|ScalarFunction $sheet_name) : IsValidExcelSheetName
{
    return new IsValidExcelSheetName($sheet_name);
}
