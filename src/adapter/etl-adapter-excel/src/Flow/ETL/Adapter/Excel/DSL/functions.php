<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\DSL;

use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\{Adapter\Excel\ExcelExtractor,
    Adapter\Excel\ExcelLoader,
    Adapter\Excel\Function\IsValidExcelSheetName,
    Attribute\DocumentationDSL,
    Attribute\Module,
    Attribute\Type as DSLType,
    Function\ScalarFunction};
use Flow\Filesystem\Path;

#[DocumentationDSL(module: Module::EXCEL, type: DSLType::EXTRACTOR)]
function from_excel(
    string|Path $path,
) : ExcelExtractor {
    return new ExcelExtractor(\is_string($path) ? path_real($path) : $path);
}

#[DocumentationDSL(module: Module::EXCEL, type: DSLType::LOADER)]
function to_excel(string|Path $path) : ExcelLoader
{
    return new ExcelLoader(\is_string($path) ? path_real($path) : $path);
}

#[DocumentationDSL(module: Module::EXCEL, type: DSLType::HELPER)]
function is_valid_excel_sheet_name(string|ScalarFunction $sheet_name) : IsValidExcelSheetName
{
    return new IsValidExcelSheetName($sheet_name);
}
