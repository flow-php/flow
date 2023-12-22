<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_exists;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class ArrayExists implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $path
    ) {
    }

    public function eval(Row $row) : bool
    {
        try {
            /** @var mixed $value */
            $value = $this->ref->eval($row);

            if (!\is_array($value)) {
                return false;
            }

            return array_dot_exists($value, $this->path);
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }
}
