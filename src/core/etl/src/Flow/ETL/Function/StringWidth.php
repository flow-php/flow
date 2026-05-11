<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\s;

final class StringWidth extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {}

    public function eval(Row $row, FlowContext $context): ?int
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('StringWidth function requires non-null value'));
        }

        return s($value)->width();
    }
}
