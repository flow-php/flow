<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_key_exists;

final class RowGroupZoneMaps implements ZoneMaps
{
    /**
     * @var array<string, null|ZoneMap>
     */
    private array $zones = [];

    public function __construct(
        private readonly RowGroupChunks $chunks,
        private readonly int $rowGroup,
    ) {}

    public function zone(FlatColumn $column): ?ZoneMap
    {
        $path = $column->flatPath();

        if (!array_key_exists($path, $this->zones)) {
            $this->zones[$path] = $this->decode($column);
        }

        return $this->zones[$path];
    }

    /**
     * Under IEEE_754_TOTAL_ORDER a writer stores a NaN bound only when every value that is not null is NaN.
     */
    private function decode(FlatColumn $column): ?ZoneMap
    {
        $statistics = $this->chunks->chunk($this->rowGroup, $column)->statistics();

        if ($statistics === null) {
            return null;
        }

        $columnOrder = $this->chunks->columnOrder($column);
        $order = PhysicalOrder::forColumn($column);
        $min = $columnOrder->trustedMin($column, $statistics->minValue($column));
        $max = $columnOrder->trustedMax($column, $statistics->maxValue($column));
        $truncatable =
            $column->type() === PhysicalType::BYTE_ARRAY || $column->type() === PhysicalType::FIXED_LEN_BYTE_ARRAY;

        return new ZoneMap(
            $min,
            $max,
            $statistics->isMinValueExact() ?? !$truncatable,
            $statistics->isMaxValueExact() ?? !$truncatable,
            $statistics->nullCount(),
            $statistics->nanCount(),
            $this->chunks->rowCount($this->rowGroup),
            $columnOrder === ColumnOrder::Ieee754TotalOrder
            && ($min !== null && $order->isNan($min) || $max !== null && $order->isNan($max)),
        );
    }
}
