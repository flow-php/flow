<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Chunk extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $size,
    ) {
    }

    /**
     * @return null|array<int, string>
     */
    public function eval(Row $row) : ?array
    {
        $value = (new Parameter($this->value))->asString($row);
        $size = (new Parameter($this->size))->asInt($row);

        if ($value === null) {
            return null;
        }

        if ($size === null || $size <= 0) {
            return [];
        }

        $chunks = s($value)->chunk($size);

        return array_map(static fn ($chunk) => $chunk->toString(), iterator_to_array($chunks));
    }
}
