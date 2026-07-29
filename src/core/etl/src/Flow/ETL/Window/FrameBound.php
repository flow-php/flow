<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class FrameBound
{
    public function __construct(
        public FrameBoundType $type,
        public ?int $offset = null,
    ) {
        $requiresOffset = $type === FrameBoundType::PRECEDING || $type === FrameBoundType::FOLLOWING;

        if ($requiresOffset && $offset === null) {
            throw new InvalidArgumentException('Frame bound of type ' . $type->name . ' requires an offset.');
        }

        if ($requiresOffset && $offset < 0) {
            throw new InvalidArgumentException('Frame bound offset must not be negative, got ' . $offset . '.');
        }

        if (!$requiresOffset && $offset !== null) {
            throw new InvalidArgumentException('Frame bound of type ' . $type->name . ' must not define an offset.');
        }
    }

    /**
     * Resolve this bound to an index in the sorted partition. Intentionally not clamped - RowsFrame
     * needs the unclamped value to detect a frame lying entirely outside the partition.
     */
    public function resolve(int $index, int $lastIndex): int
    {
        return match ($this->type) {
            FrameBoundType::UNBOUNDED_PRECEDING => 0,
            FrameBoundType::PRECEDING => $index - (int) $this->offset,
            FrameBoundType::CURRENT_ROW => $index,
            FrameBoundType::FOLLOWING => $index + (int) $this->offset,
            FrameBoundType::UNBOUNDED_FOLLOWING => $lastIndex,
        };
    }
}
