<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use function Flow\ArrayDot\array_dot_exists;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayExists implements Expression
{
    public function __construct(
        private readonly Expression $reference,
        private readonly string $path
    ) {
    }

    public function eval(Row $row) : bool
    {
        try {
            $ref = $row->get($this->reference);

            if (!\is_array($ref->value())) {
                return false;
            }

            return array_dot_exists($ref->value(), $this->path);
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }
}
