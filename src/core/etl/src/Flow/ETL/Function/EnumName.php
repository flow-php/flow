<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use UnitEnum;

use function Flow\Types\DSL\type_string;

final class EnumName extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
    ) {}

    public function eval(Row $row, FlowContext $context): ?ScalarResult
    {
        $enum = (new Parameter($this->value))->eval($row, $context);

        if (!$enum instanceof UnitEnum) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('EnumName function requires a UnitEnum value'));
        }

        return new ScalarResult($enum->name, type_string());
    }
}
