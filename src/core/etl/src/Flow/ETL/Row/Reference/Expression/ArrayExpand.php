<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\ExpandResults;
use Flow\ETL\Row\Reference\Expression;

final class ArrayExpand implements ExpandResults, Expression
{
    public function __construct(private readonly Expression $ref, private readonly ArrayExpand\ArrayExpand $expand)
    {
    }

    public function eval(Row $row) : mixed
    {
        $array = $this->ref->eval($row);

        if (!\is_array($array)) {
            throw new RuntimeException(\get_class($this->ref) . ' is not an array, got: ' . \gettype($array));
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
