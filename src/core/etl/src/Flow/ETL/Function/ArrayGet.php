<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_get;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class ArrayGet implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $path
    ) {
    }

    public function eval(Row $row) : mixed
    {
        try {
            /** @var mixed $value */
            $value = $this->ref->eval($row);

            if (!\is_array($value)) {
                return null;
            }

            return array_dot_get($value, $this->path);
        } catch (InvalidArgumentException $e) {
            return null;
        }
    }
}
