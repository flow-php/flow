<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use Flow\ETL\Row;

final class ArrayDotValueLessThan implements RowCondition
{
    public function __construct(
        private readonly string $arrayEntryName,
        private readonly string $path,
        private readonly mixed $value
    ) {
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->arrayEntryName)) {
            return false;
        }

        if (!$row->entries()->get($this->arrayEntryName) instanceof Row\Entry\ArrayEntry) {
            return false;
        }

        if (!array_dot_exists((array) $row->valueOf($this->arrayEntryName), $this->path)) {
            return false;
        }

        return array_dot_get((array) $row->valueOf($this->arrayEntryName), $this->path) < $this->value;
    }
}
