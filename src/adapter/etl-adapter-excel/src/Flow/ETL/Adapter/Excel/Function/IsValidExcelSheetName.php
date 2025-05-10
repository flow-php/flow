<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Function;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Function\{Parameter, ScalarFunction};
use Flow\ETL\Row;

final readonly class IsValidExcelSheetName implements ScalarFunction
{
    public function __construct(private ScalarFunction|string $sheetName)
    {
    }

    public function eval(Row $row) : mixed
    {
        $sheetName = (new Parameter($this->sheetName))->asString($row);

        if ($sheetName === null) {
            return false;
        }

        return SheetNameAssertion::isValid($sheetName);
    }
}
