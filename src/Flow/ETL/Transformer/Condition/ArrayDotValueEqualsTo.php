<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use Flow\ETL\Row;

final class ArrayDotValueEqualsTo implements RowCondition
{
    private string $arrayEntryName;

    private string $path;

    /**
     * @var mixed
     */
    private $value;

    private bool $identical;

    /**
     * @param string $arrayEntryName
     * @param string $path
     * @param mixed $value
     * @param bool $identical
     */
    public function __construct(string $arrayEntryName, string $path, $value, bool $identical = true)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->path = $path;
        $this->value = $value;
        $this->identical = $identical;
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

        return $this->identical
            ? array_dot_get((array) $row->valueOf($this->arrayEntryName), $this->path) === $this->value
            : array_dot_get((array) $row->valueOf($this->arrayEntryName), $this->path) == $this->value;
    }
}
