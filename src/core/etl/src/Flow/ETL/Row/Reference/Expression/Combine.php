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
        $extractor = new Row\Reference\ValueExtractor();

        $keys = $extractor->value($row, $this->keys);
        $values = $extractor->value($row, $this->values);

        if (!\is_array($keys) || !\is_array($values)) {
            return null;
        }

        if (\count($keys) !== \count($values)) {
            return null;
        }

        if (!\array_is_list($keys)) {
            return null;
        }

        /** @psalm-suppress PossiblyUndefinedArrayOffset */
        if (!\is_string($keys[0]) && !\is_int($keys[0])) {
            return null;
        }

        /** @var array<array-key, array-key> $keys */
        return \array_combine($keys, $values);
    }
}
