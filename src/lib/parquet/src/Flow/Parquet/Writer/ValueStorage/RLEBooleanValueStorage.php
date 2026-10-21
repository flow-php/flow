<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;

use function array_filter;
use function array_map;
use function array_push;
use function count;
use function intdiv;

final class RLEBooleanValueStorage implements ValueStorage
{
    /**
     * @var list<int>
     */
    private array $bits = [];

    public function __construct(
        private readonly RLEBitPackedPacker $packer = new RLEBitPackedPacker(new RLEBitPackedHybrid()),
    ) {}

    public function addValues(FlatColumn $column, array $values): void
    {
        array_push(
            $this->bits,
            ...array_map(static fn(bool $value): int => $value ? 1 : 0, array_filter($values, is_bool(...))),
        );
    }

    public function encoding(): Encodings
    {
        return Encodings::RLE;
    }

    public function getBuffer(): string
    {
        return $this->packer->packWithLength(1, $this->bits);
    }

    public function isEmpty(): bool
    {
        return $this->bits === [];
    }

    public function reset(): void
    {
        $this->bits = [];
    }

    public function plainSize(): int
    {
        return intdiv(count($this->bits) + 7, 8);
    }
}
