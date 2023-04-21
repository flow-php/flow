<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArraySort implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly \Closure $function
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_array($val)) {
            return null;
        }

        $this->recursiveSort($val, $this->function);

        return $val;
    }

    private function recursiveSort(array &$array, \Closure $function) : void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if (\is_array($value)) {
                $this->recursiveSort($value, $function);
            }
        }

        $function($array);
    }
}
