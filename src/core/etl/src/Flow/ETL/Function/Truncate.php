<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Truncate extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $length,
        private readonly ScalarFunction|string $ellipsis = '...',
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $length = (new Parameter($this->length))->asInt($row, $context);
        $ellipsis = (new Parameter($this->ellipsis))->asString($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Truncate function requires non-null value'));
        }

        if ($length === null) {
            return $value;
        }

        return s($value)->truncate($length, $ellipsis ?? '...')->toString();
    }
}
