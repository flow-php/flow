<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class ReadPlan
{
    /**
     * @param RowSelection $selection rows to read
     * @param RowSelection $matched rows proven to match the filter, within $selection
     */
    public function __construct(
        public RowSelection $selection,
        public RowSelection $matched,
    ) {
        if ($selection->totalRows() !== $matched->totalRows()) {
            throw new InvalidArgumentException('A read plan selection and its matched rows must cover the same rows');
        }

        if (!$selection->contains($matched)) {
            throw new InvalidArgumentException('A read plan can match only rows it selects');
        }
    }

    /**
     * Every row of the window, each a match.
     */
    public static function window(int $rows, int $offset = 0, ?int $limit = null): self
    {
        $selection = RowSelection::window($rows, $offset, $limit);

        return new self($selection, $selection);
    }
}
