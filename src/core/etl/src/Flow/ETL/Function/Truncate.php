<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Symfony\Component\String\s;

final class Truncate extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $length,
        private readonly ScalarFunction|string $ellipsis = '...',
    ) {}

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $length = (new Parameter($this->length))->asInt($row, $context);
        $ellipsis = (new Parameter($this->ellipsis))->asString($row, $context);

        if ($value === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Truncate function requires non-null value'));
        }

        if ($length === null) {
            return $value;
        }

        return s($value)->truncate($length, $ellipsis ?? '...')->toString();
    }
}
