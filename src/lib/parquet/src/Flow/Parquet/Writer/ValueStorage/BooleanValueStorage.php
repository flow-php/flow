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

    /**
     * @param array<bool> $values
     */
    #[\Override]
    public function addValues(FlatColumn $column, array $values) : void
    {
        $nonNullValues = \array_filter($values, static fn (?bool $value) => $value !== null);

        foreach ($nonNullValues as $value) {
            $this->values[] = $value;
        }
    }

    #[\Override]
    public function getBuffer() : string
    {
        if (empty($this->values)) {
            return '';
        }

        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $writer->writeBooleans($this->values);

        return $buffer;
    }

    #[\Override]
    public function isEmpty() : bool
    {
        return !\count($this->values);
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
