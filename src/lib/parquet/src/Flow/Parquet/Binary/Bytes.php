<?php

declare(strict_types=1);

namespace Flow\Parquet\Binary;

use Flow\Parquet\DataSize;

/**
 * @implements \ArrayAccess<int, int>
 * @implements \IteratorAggregate<int, int>
 */
final class Bytes implements \ArrayAccess, \Countable, \IteratorAggregate
{
    /** @var ?\ArrayIterator<int, int> */
    private ?\ArrayIterator $iterator = null;

    private readonly DataSize $size;

    /**
     * @param array<int, int> $bytes
     */
    public function __construct(
        private array $bytes,
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {
        $this->size = new DataSize(\count($this->bytes) * 8);
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

    /**
     * @return \ArrayIterator<int, int>
     */
    public function getIterator() : \ArrayIterator
    {
        if ($this->iterator === null) {
            $this->iterator = new \ArrayIterator($this->bytes);
        }

        return $this->iterator;
    }

    public function offsetExists(mixed $offset) : bool
    {
        return isset($this->bytes[$offset]);
    }

    public function offsetGet(mixed $offset) : int
    {
        return $this->bytes[$offset];
    }

    public function offsetSet(mixed $offset, mixed $value) : void
    {
        if ($offset === null) {
            $this->bytes[] = $value;
        } else {
            $this->bytes[$offset] = $value;
        }
    }

    public function offsetUnset(mixed $offset) : void
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
