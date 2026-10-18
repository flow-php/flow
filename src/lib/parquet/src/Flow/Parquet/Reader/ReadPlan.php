<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Index\Indexes;

final readonly class ReadPlan
{
    /**
     * @param RowSelection $selection rows to read
     * @param RowSelection $matched rows proven to match the filter, within $selection
     * @param Indexes $indexes the file's page indexes, read once for planning and reading
     */
    public function __construct(
        public RowSelection $selection,
        public RowSelection $matched,
        public Indexes $indexes,
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
    public static function window(int $rows, Indexes $indexes, int $offset = 0, ?int $limit = null): self
    {
        $selection = RowSelection::window($rows, $offset, $limit);

        return new self($selection, $selection, $indexes);
    }
}
