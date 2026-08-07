<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use BackedEnum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function is_int;

final class EnumValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
    ) {}

    public function eval(Row $row, FlowContext $context): ?ScalarResult
    {
        $enum = (new Parameter($this->value))->eval($row, $context);

        if (!$enum instanceof BackedEnum) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('EnumValue function requires a BackedEnum value'));
        }

        return new ScalarResult($enum->value, is_int($enum->value) ? type_integer() : type_string());
    }
}
