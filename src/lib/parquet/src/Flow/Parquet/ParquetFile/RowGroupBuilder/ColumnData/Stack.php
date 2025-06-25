<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;

final class Stack
{
    /**
     * @var array<array-key, mixed>
     */
    private array $stack;

    public function __construct(
        private readonly int $maxRepetitionLevel,
    ) {
        $this->stack = [];
    }

    /**
     * @return array<array-key, mixed>
     */
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
                if (!\is_array($value)) {
                    throw new InvalidArgumentException(\sprintf('Expected array for value, got %s', \get_debug_type($value)));
                }

                if (!\count($value)) {
                    throw new InvalidArgumentException('Cannot access last element of empty array');
                }
                $valueNode = &$value[\count($value) - 1];
            } else {
                if (!\is_array($valueNode)) {
                    throw new InvalidArgumentException(\sprintf('Expected array for value node, got %s', \get_debug_type($valueNode)));
                }

                if (!\count($valueNode)) {
                    throw new InvalidArgumentException('Cannot access last element of empty array');
                }
                $valueNode = &$valueNode[\count($valueNode) - 1];
            }

            if (!\is_array($lastStackNode)) {
                throw new InvalidArgumentException(\sprintf('Expected array for last stack node, got %s', \get_debug_type($lastStackNode)));
            }

            if (!\count($lastStackNode)) {
                throw new InvalidArgumentException('Cannot access last element of empty array');
            }
            $lastStackNode = &$lastStackNode[\count($lastStackNode) - 1];
        }
        $valueNode ??= $value;

        if (!\is_array($lastStackNode)) {
            throw new InvalidArgumentException(\sprintf('Expected array for last stack node, got %s', \get_debug_type($lastStackNode)));
        }

        if (!\is_array($valueNode)) {
            throw new InvalidArgumentException(\sprintf('Expected array for value node, got %s', \get_debug_type($valueNode)));
        }
        $lastStackNode = \array_merge($lastStackNode, $valueNode);

        unset($lastStackNode, $valueNode);
    }
}
