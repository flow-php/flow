<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function gettype;
use function sprintf;

final class DeltaBinaryPackedValueStorage implements ValueStorage
{
    /**
     * @var array<int>
     */
    private array $values = [];

    private PhysicalType $physicalType = PhysicalType::INT64;

    private int $size = 0;

    public function __construct(
        private readonly DeltaBinaryPackedEncoder $int32Encoder = new DeltaBinaryPackedEncoder(
            physicalType: PhysicalType::INT32,
        ),
        private readonly DeltaBinaryPackedEncoder $int64Encoder = new DeltaBinaryPackedEncoder(
            physicalType: PhysicalType::INT64,
        ),
    ) {}

    public function addValues(FlatColumn $column, array $values): void
    {
        if (!in_array($column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
            throw new InvalidArgumentException('Delta encoding only supports INT32 and INT64 physical types');
        }

        $this->physicalType = $column->type();
        $valueSize = $this->physicalType === PhysicalType::INT32 ? 4 : 8;

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($value !== null) {
                if (!is_int($value)) {
                    throw new InvalidArgumentException(sprintf(
                        'Delta encoding requires integer values, got %s',
                        gettype($value),
                    ));
                }

                $this->values[] = $value;
                $this->size += $valueSize;
            }
        }
    }

    public function encoding(): Encodings
    {
        return Encodings::DELTA_BINARY_PACKED;
    }

    public function getBuffer(): string
    {
        return ($this->physicalType === PhysicalType::INT32
            ? $this->int32Encoder
            : $this->int64Encoder)->encode($this->values);
    }

    public function isEmpty(): bool
    {
        return empty($this->values);
    }

    public function reset(): void
    {
        $this->values = [];
        $this->size = 0;
    }

    public function plainSize(): int
    {
        return $this->size;
    }
}
