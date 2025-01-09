<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;

final class Stack
{
    private array $stack;

    public function __construct(
        private readonly int $maxRepetitionLevel,
    ) {
        $this->stack = [];
    }

    public function dump() : array
    {
        return $this->stack;
    }

    public function push(int $level, mixed $value) : void
    {
        if ($level > $this->maxRepetitionLevel) {
            throw new InvalidArgumentException('Given level "' . $level . '"  is greater than max level, "' . $this->maxRepetitionLevel . '"');
        }

        if ($this->maxRepetitionLevel === 0 || $level === 0) {
            $this->stack[] = $value;

            return;
        }

        $valueNode = null;
        $lastStackNode = &$this->stack[\count($this->stack) - 1];

        for ($l = 1; $l < $level; $l++) {
            if ($valueNode === null) {
                $valueNode = &$value[\count($value) - 1];
            } else {
                $valueNode = &$valueNode[\count($valueNode) - 1];
            }

            $lastStackNode = &$lastStackNode[\count($lastStackNode) - 1];
        }
        $valueNode ??= $value;

        $lastStackNode = \array_merge($lastStackNode, $valueNode);

        unset($lastStackNode, $valueNode);
    }
}
