<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\u;

final class UnicodeLength extends ScalarFunctionChain
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
                ->invalidResult(new InvalidArgumentException('UnicodeLength function requires non-null value'));
        }

        return u($value)->length();
    }
}
