<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Repeat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $times,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $times = (new Parameter($this->times))->asInt($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Repeat function requires non-null value'));
        }

        if ($times === null || $times <= 0) {
            $context->functions()->invalidResult(new InvalidArgumentException('Repeat function requires non-null, positive times'));

            return '';
        }

        return s($value)->repeat($times)->toString();
    }
}
