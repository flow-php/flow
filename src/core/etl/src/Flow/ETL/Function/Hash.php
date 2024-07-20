<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\ETL\Row;

final class Hash extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly Algorithm $algorithm = new NativePHPHash(),
    ) {
    }

    public function eval(Row $row) : ?string
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match ($value) {
            null => null,
            default => match (\gettype($value)) {
                'array', 'object' => $this->algorithm->hash(\serialize($value)),
                default => $this->algorithm->hash((string) $value),
            }
        };
    }
}
