<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

/**
 * The statistics a writer computes, read from Options once for every column chunk builder.
 */
final readonly class StatisticsOptions
{
    public function __construct(
        private StatisticsLevel $level,
        private bool $pageHeaderStatistics,
        private StatisticsTruncation $truncation,
        private StatisticsTruncation $columnIndexTruncation,
    ) {}

    public static function fromOptions(Options $options): self
    {
        $level = $options->getEnum(Option::STATISTICS_LEVEL, StatisticsLevel::class);

        return new self(
            $level,
            $level === StatisticsLevel::PAGE && $options->getBool(Option::PAGE_HEADER_STATISTICS),
            new StatisticsTruncation($options->getInt(Option::STATISTICS_TRUNCATE_LENGTH)),
            new StatisticsTruncation(
                $options->getInt(Option::COLUMN_INDEX_TRUNCATE_LENGTH),
                Option::COLUMN_INDEX_TRUNCATE_LENGTH,
            ),
        );
    }

    /**
     * The column chunk's statistics, none at StatisticsLevel::NONE.
     */
    public function chunk(StatisticsCounter $chunk): ?Statistics
    {
        return $this->level === StatisticsLevel::NONE ? null : $chunk->toStatistics();
    }

    public function counter(FlatColumn $column): StatisticsCounter
    {
        return new StatisticsCounter($column, $this->truncation);
    }

    /**
     * A data page header's statistics, only at StatisticsLevel::PAGE with Option::PAGE_HEADER_STATISTICS.
     */
    public function pageHeader(StatisticsCounter $page): ?Statistics
    {
        return $this->pageHeaderStatistics ? $page->toPageStatistics() : null;
    }

    /**
     * A new page index for one column chunk.
     */
    public function pageIndex(FlatColumn $column): PageIndexBuilder
    {
        return new PageIndexBuilder($column, $this->level, $this->columnIndexTruncation);
    }
}
