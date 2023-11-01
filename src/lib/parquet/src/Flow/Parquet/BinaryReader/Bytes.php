<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\ByteOrder;
use Flow\Parquet\DataSize;

final class Bytes implements \ArrayAccess, \Countable, \IteratorAggregate
{
    private readonly \ArrayIterator $iterator;

    private readonly DataSize $size;

    public function __construct(
        private array $bytes,
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN
    ) {
        $this->size = new DataSize(\count($this->bytes) * 8);
        $this->iterator = new \ArrayIterator($this->bytes);
    }

    public static function fromString(string $string, ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN) : self
    {
        /** @phpstan-ignore-next-line */
        return new self(\array_values(\unpack('C*', $string)), $byteOrder);
    }

    // Countable methods
    public function count() : int
    {
        return \count($this->bytes);
    }

    // IteratorAggregate methods
    public function getIterator() : \ArrayIterator
    {
        return $this->iterator;
    }

    // ArrayAccess methods
    public function offsetExists($offset) : bool
    {
        return isset($this->bytes[$offset]);
    }

    public function offsetGet($offset) : mixed
    {
        return $this->bytes[$offset];
    }

    public function offsetSet($offset, $value) : void
    {
        if ($offset === null) {
            $this->bytes[] = $value;
        } else {
            $this->bytes[$offset] = $value;
        }
    }

    public function offsetUnset($offset) : void
    {
        unset($this->bytes[$offset]);
    }

    public function size() : DataSize
    {
        return $this->size;
    }

    /**
     * @return array<int>
     */
    public function toArray() : array
    {
        return $this->bytes;
    }

    /**
     * Convert bytes to a single integer.
     */
    public function toInt() : int
    {
        $result = 0;
        $bytes = $this->byteOrder === ByteOrder::LITTLE_ENDIAN ? $this->bytes : \array_reverse($this->bytes);

        foreach ($bytes as $shift => $byte) {
            $result |= ($byte << ($shift * 8));
        }

        return $result;
    }

    public function toString() : string
    {
        return \pack('C*', ...$this->bytes);
    }
}
