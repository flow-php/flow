<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayExpand implements ExpandResults, ScalarFunction
{
    public function __construct(private readonly ScalarFunction $ref, private readonly ArrayExpand\ArrayExpand $expand)
    {
    }

    public function eval(Row $row) : mixed
    {
        $array = $this->ref->eval($row);

        if (!\is_array($array)) {
            return null;
        }

        if ($this->expand === ArrayExpand\ArrayExpand::KEYS) {
            return \array_keys($array);
        }

        if ($this->expand === ArrayExpand\ArrayExpand::BOTH) {
            return \array_map(fn ($key, $value) => [$key => $value], \array_keys($array), $array);
        }

        return $array;
    }

    public function expand() : bool
    {
        return true;
    }
}
