<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class BooleanValueStorage implements ValueStorage
{
    /**
     * @var array<bool>
     */
    private array $values = [];

    public function addValues(FlatColumn $column, array $values): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if (\is_bool($value)) {
                $this->values[] = $value;
            }
        }
    }

    public function getBuffer(): string
    {
        if (empty($this->values)) {
            return '';
        }

        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $bits = [];

        foreach ($this->values as $value) {
            $bits[] = $value ? 1 : 0;
        }
        $writer->writeBits($bits);

        return $buffer;
    }

    public function isEmpty(): bool
    {
        return !\count($this->values);
    }

    public function reset(): void
    {
        $this->values = [];
    }

    public function size(): int
    {
        return \count($this->values);
    }
}
