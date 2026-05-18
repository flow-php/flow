<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function mb_strtoupper;

final class ToUpper extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ToUpper function requires non-null value'));
        }

        return mb_strtoupper($value);
    }
}
