<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class ToLower extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ToLower function requires non-null value'));
        }

        return \strtolower($value);
    }
}
