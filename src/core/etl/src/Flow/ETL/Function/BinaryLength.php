<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\b;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class BinaryLength extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?int
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('BinaryLength function requires non-null value'));
        }

        return b($value)->length();
    }
}
