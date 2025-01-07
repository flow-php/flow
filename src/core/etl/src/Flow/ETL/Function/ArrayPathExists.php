<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ArrayDot\array_dot_exists;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;

final class ArrayPathExists extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction|string $path,
    ) {
    }

    public function eval(Row $row) : bool
    {
        try {
            $array = (new Parameter($this->array))->asArray($row);
            $path = (new Parameter($this->path))->asString($row);

            if ($array === null || $path === null) {
                return false;
            }

            return array_dot_exists($array, $path);
        } catch (InvalidArgumentException) {
            return false;
        }
    }
}
