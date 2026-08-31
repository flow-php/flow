<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use ArrayAccess;
use ArrayIterator;
use Countable;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use IteratorAggregate;
use Traversable;

use function array_key_exists;
use function array_values;
use function count;

/**
 * @implements \ArrayAccess<int, Partition>
 * @implements \IteratorAggregate<int, Partition>
 */
final readonly class Partitions implements ArrayAccess, Countable, IteratorAggregate
{
    /**
     * @var array<int, Partition>
     */
    private array $partitions;

    public function __construct(Partition ...$partitions)
    {
        $names = [];

        foreach ($partitions as $partition) {
            if (array_key_exists($partition->name, $names)) {
                throw new InvalidArgumentException(
                    "Partition \"{$partition->name}\" is declared more than once, a column partitions a dataset once",
                );
            }

            $names[$partition->name] = true;
        }

        $this->partitions = array_values($partitions);
    }

    public function count(): int
    {
        return count($this->partitions);
    }

    public function get(string $name): Partition
    {
        foreach ($this->partitions as $partition) {
            if ($partition->name === $name) {
                return $partition;
            }
        }

        throw new InvalidArgumentException("Partition with name: '{$name}' not found");
    }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->partitions);
    }

    public function has(string $name): bool
    {
        foreach ($this->partitions as $partition) {
            if ($partition->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function offsetExists(mixed $offset): bool
    {
        return array_key_exists($offset, $this->partitions);
    }

    /**
     * @return Partition
     */
    public function offsetGet(mixed $offset): mixed
    {
        return $this->partitions[$offset];
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        throw new RuntimeException('Partitions are immutable');
    }

    public function offsetUnset(mixed $offset): void
    {
        throw new RuntimeException('Partitions are immutable');
    }

    /**
     * @return array<Partition>
     */
    public function toArray(): array
    {
        return $this->partitions;
    }
}
