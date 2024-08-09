<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Combine extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|array $keys,
        private readonly ScalarFunction|array $values,
    ) {
    }

    public function eval(Row $row) : ?array
    {
        $keys = (new Parameter($this->keys))->asArray($row);
        $values = (new Parameter($this->values))->asArray($row);

        if (null === $keys || null === $values) {
            return null;
        }

        if ([] === $keys) {
            return [];
        }

        if (!\array_is_list($keys)) {
            return null;
        }

        if (\count($keys) !== \count($values)) {
            return null;
        }

        if (!\is_string($keys[0] ?? null) && !\is_int($keys[0] ?? null)) {
            return null;
        }

        /** @var array<array-key, array-key> $keys */
        return \array_combine($keys, $values);
    }
}
