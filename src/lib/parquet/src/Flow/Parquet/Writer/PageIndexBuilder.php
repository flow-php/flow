<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

/**
 * One column chunk's page index, page by page, as arrow-rs builds it: a column index only at StatisticsLevel::PAGE and
 * only while every page that holds a value has bounds, an offset index always.
 */
final class PageIndexBuilder
{
    private bool $ascending = true;

    private readonly PhysicalCodec $codec;

    private bool $descending = true;

    private bool|float|int|string|null $lastMax = null;

    private bool|float|int|string|null $lastMin = null;

    /**
     * @var list<string>
     */
    private array $maxValues = [];

    /**
     * @var list<string>
     */
    private array $minValues = [];

    /**
     * @var list<int>
     */
    private array $nanCounts = [];

    /**
     * @var list<int>
     */
    private array $nullCounts = [];

    /**
     * @var list<bool>
     */
    private array $nullPages = [];

    private readonly PhysicalOrder $order;

    /**
     * @var list<int>
     */
    private array $pageRows = [];

    private bool $valid;

    public function __construct(
        private readonly FlatColumn $column,
        StatisticsLevel $level,
        private readonly StatisticsTruncation $truncation,
    ) {
        $this->codec = new PhysicalCodec();
        $this->order = PhysicalOrder::forColumn($column);
        $this->valid = $level === StatisticsLevel::PAGE;
    }

    public function addPage(StatisticsCounter $statistics, int $rows): void
    {
        $this->pageRows[] = $rows;

        if (!$this->valid) {
            return;
        }

        $this->nullCounts[] = $statistics->nullCount();
        $this->nanCounts[] = (int) $statistics->nanCount();

        if ($statistics->notNullCount() === 0) {
            $this->nullPages[] = true;
            $this->minValues[] = '';
            $this->maxValues[] = '';

            return;
        }

        $min = $statistics->min();
        $max = $statistics->max();

        if ($min === null || $max === null) {
            $this->valid = false;

            return;
        }

        if ($this->lastMin !== null && $this->lastMax !== null) {
            $this->ascending =
                $this->ascending
                && $this->order->compare($this->lastMin, $min) <= 0
                && $this->order->compare($this->lastMax, $max) <= 0;
            $this->descending =
                $this->descending
                && $this->order->compare($min, $this->lastMin) <= 0
                && $this->order->compare($max, $this->lastMax) <= 0;
        }

        $this->lastMin = $min;
        $this->lastMax = $max;
        $this->nullPages[] = false;
        $this->minValues[] = $this->truncation->min($this->column, $this->codec->encodeStatistic($this->column, $min));
        $this->maxValues[] = $this->truncation->max($this->column, $this->codec->encodeStatistic($this->column, $max));
    }

    public function columnIndex(): ?ColumnIndex
    {
        if (!$this->valid || $this->nullPages === []) {
            return null;
        }

        return new ColumnIndex(
            $this->nullPages,
            $this->minValues,
            $this->maxValues,
            match (true) {
                $this->ascending => BoundaryOrder::ASCENDING,
                $this->descending => BoundaryOrder::DESCENDING,
                default => BoundaryOrder::UNORDERED,
            },
            $this->nullCounts,
            $this->order->isFloatingPoint() ? $this->nanCounts : null,
        );
    }

    /**
     * The data pages written from $firstDataPageOffset on, header included, one per addPage() call.
     */
    public function offsetIndex(int $firstDataPageOffset, PageContainers $pages): OffsetIndex
    {
        $locations = [];
        $offset = $firstDataPageOffset;
        $firstRow = 0;

        foreach ($pages->dataPageContainers() as $page => $container) {
            $locations[] = new PageLocation($offset, $container->totalCompressedSize(), $firstRow);
            $offset += $container->totalCompressedSize();
            $firstRow += $this->pageRows[$page];
        }

        return new OffsetIndex($locations);
    }
}
