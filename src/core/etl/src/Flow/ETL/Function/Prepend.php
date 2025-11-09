<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Prepend extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $prefix,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $prefix = (new Parameter($this->prefix))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Prepend function requires non-null value'));
        }

        if ($prefix === null) {
            return $value;
        }

        return s($value)->prepend($prefix)->toString();
    }
}
