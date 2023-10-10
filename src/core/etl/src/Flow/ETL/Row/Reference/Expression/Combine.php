<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Combine implements Expression
{
    public function __construct(
        private readonly Expression $keys,
        private readonly Expression $values,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $keys = $this->keys->eval($row);
        $values = $this->values->eval($row);

        if (!\is_array($keys) || !\is_array($values)) {
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
