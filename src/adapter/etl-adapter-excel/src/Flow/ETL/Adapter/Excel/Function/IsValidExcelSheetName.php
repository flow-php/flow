<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Function;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Function\{Parameter, ScalarFunction};

final readonly class IsValidExcelSheetName implements ScalarFunction
{
    public function __construct(private ScalarFunction|string $sheetName)
    {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $sheetName = (new Parameter($this->sheetName))->asString($row, $context);

        if ($sheetName === null) {
            return false;
        }

        return SheetNameAssertion::isValid($sheetName);
    }
}
