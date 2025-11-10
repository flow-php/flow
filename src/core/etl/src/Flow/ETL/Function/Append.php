<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Append extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $suffix,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $suffix = (new Parameter($this->suffix))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Append function requires non-null value'));
        }

        if ($suffix === null) {
            return $value;
        }

        return s($value)->append($suffix)->toString();
    }
}
