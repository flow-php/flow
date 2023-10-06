<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArraySort implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly Expression\ArraySort\Sort $function,
        private readonly ?int $flags,
        private readonly bool $recursive
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (!\is_array($val)) {
            return null;
        }

        $this->recursiveSort($val, $this->function->value, $this->flags, $this->recursive);

        return $val;
    }

    private function recursiveSort(array &$array, callable $function, ?int $flags, bool $recursive) : void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if ($recursive && \is_array($value)) {
                $this->recursiveSort($value, $function, $flags, true);
            }
        }

        if (null !== $flags) {
            $function($array, $flags);
        } else {
            $function($array);
        }
    }
}
