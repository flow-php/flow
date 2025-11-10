<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\u;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class UnicodeLength extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?int
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('UnicodeLength function requires non-null value'));
        }

        return u($value)->length();
    }
}
