<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use function Flow\ArrayDot\array_dot_exists;
use Flow\ETL\Row;

final class ArrayDotExists implements RowCondition
{
    private string $arrayEntryName;

    private string $path;

    public function __construct(string $arrayEntryName, string $path)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->path = $path;
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->arrayEntryName)) {
            return false;
        }

        if (!$row->entries()->get($this->arrayEntryName) instanceof Row\Entry\ArrayEntry) {
            return false;
        }

        return array_dot_exists((array) $row->valueOf($this->arrayEntryName), $this->path);
    }
}
