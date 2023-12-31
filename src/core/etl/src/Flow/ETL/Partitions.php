<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\RuntimeException;

/**
 * @implements \ArrayAccess<int, Partition>
 * @implements \IteratorAggregate<int, Partition>
 */
final class Partitions implements \ArrayAccess, \Countable, \IteratorAggregate
{
    private readonly array $partitions;

    public function __construct(Partition ...$partitions)
    {
        \uasort($partitions, static fn (Partition $a, Partition $b) => $a->name <=> $b->name);

        $this->partitions = $partitions;
    }

    public function __serialize() : array
    {
        return [
            'partitions' => $this->partitions,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->partitions = $data['partitions'];
    }

    public function count() : int
    {
        return \count($this->partitions);
    }

    public function getIterator() : \Traversable
    {
        return new \ArrayIterator($this->partitions);
    }

    public function has(string $name) : bool
    {
        foreach ($this->partitions as $partition) {
            if ($partition->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function id() : string
    {
        $id = '|';

        foreach ($this->partitions as $partition) {
            $id .= $partition->name . '_' . $partition->value . '|';
        }

        return \hash('xxh128', $id);
    }

    public function offsetExists(mixed $offset) : bool
    {
        return \array_key_exists($offset, $this->partitions);
    }

    public function offsetGet(mixed $offset) : mixed
    {
        return $this->partitions[$offset];
    }

    public function offsetSet(mixed $offset, mixed $value) : void
    {
        throw new RuntimeException('Partitions are immutable');
    }

    public function offsetUnset(mixed $offset) : void
    {
        throw new RuntimeException('Partitions are immutable');
    }

    /**
     * @return array<Partition>
     */
    public function toArray() : array
    {
        return $this->partitions;
    }
}
