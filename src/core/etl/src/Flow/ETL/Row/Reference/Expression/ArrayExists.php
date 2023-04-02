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
        private readonly Expression $ref,
        private readonly string $path
    ) {
    }

    public function eval(Row $row) : bool
    {
        try {
            /** @var mixed $value */
            $value = (new Row\Reference\ValueExtractor())->value($row, $this->ref);

            if (!\is_array($value)) {
                return false;
            }

            return array_dot_exists($value, $this->path);
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }
}
