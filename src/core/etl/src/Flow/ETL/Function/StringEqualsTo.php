<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class StringEqualsTo extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $string,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?bool
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $string = (new Parameter($this->string))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringEqualsTo function requires non-null value'));
        }

        if ($string === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('StringEqualsTo function requires non-null string'));
        }

        return s($value)->equalsTo($string);
    }
}
