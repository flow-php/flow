<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};

final class DeltaBinaryPackedValueStorage implements ValueStorage
{
    /**
     * @var array<int>
     */
    private array $values = [];

    #[\Override]
    public function addValues(FlatColumn $column, array $values) : void
    {
        if (!in_array($column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
            throw new InvalidArgumentException('Delta encoding only supports INT32 and INT64 physical types');
        }

        foreach ($values as $value) {
            if ($value !== null) {
                if (!is_int($value)) {
                    throw new InvalidArgumentException(\sprintf('Delta encoding requires integer values, got %s', \gettype($value)));
                }

                $this->values[] = $value;
            }
        }
    }

    #[\Override]
    public function getBuffer() : string
    {
        if (!\count($this->values)) {
            return '';
        }

        return (new DeltaBinaryPackedEncoder())->encode($this->values);
    }

    #[\Override]
    public function isEmpty() : bool
    {
        return empty($this->values);
    }

    #[\Override]
    public function reset() : void
    {
        $this->values = [];
    }

    #[\Override]
    public function size() : int
    {
        return \count($this->values);
    }
}
