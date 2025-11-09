<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\{FlowContext, Row};

final class IsEmpty extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?bool
    {
        $value = (new Parameter($this->value))->asString($row, $context);

        if ($value === null) {
            return null;
        }

        return s($value)->isEmpty();
    }
}
