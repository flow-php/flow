<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\Types\Value\Json;

final class Hash extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly Algorithm $algorithm = new NativePHPHash(),
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?string
    {
        $value = (new Parameter($this->value))->eval($row, $context);

        if ($value instanceof Json) {
            $value = $value->toArray();
        }

        return match ($value) {
            null => null,
            default => match (\gettype($value)) {
                'array', 'object' => $this->algorithm->hash(\serialize($value)),
                default => $this->algorithm->hash(\is_scalar($value) ? (string) $value : ''),
            },
        };
    }
}
