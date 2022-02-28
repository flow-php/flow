<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\RuntimeException;
use Flow\Serializer\Serializable;

/**
 * @implements \ArrayAccess<string, Entry>
 * @implements \IteratorAggregate<string, Entry>
 * @psalm-immutable
 */
final class Entries implements \ArrayAccess, \Countable, \IteratorAggregate, Serializable
{
    /**
     * @var array<string, Entry>
     */
    private array $entries;

    public function __construct(Entry ...$entries)
    {
        $this->entries = [];

        if (\count($entries)) {
            foreach ($entries as $entry) {
                $this->entries[$entry->name()] = $entry;
            }

            if (\count($this->entries) !== \count($entries)) {
                throw InvalidArgumentException::because(\sprintf('Entry names must be unique, given: [%s]', \implode(', ', \array_map(fn (Entry $entry) => $entry->name(), $entries))));
            }
        }
    }

    /**
     * @return array{entries: array<string, Entry>}
     */
    public function __serialize() : array
    {
        return ['entries' => $this->entries];
    }

    /**
     * @param array{entries: array<string, Entry>} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entries = $data['entries'];
    }

    public function add(Entry ...$entries) : self
    {
        $newEntries = [];

        foreach ($entries as $entry) {
            $newEntries[$entry->name()] = $entry;
        }

        $mergedEntries = \array_merge($this->entries, $newEntries);

        if (\count($mergedEntries) !== \count($entries) + \count($this->entries)) {
            throw InvalidArgumentException::because(
                \sprintf(
                    'Added entries names must be unique, given: [%s] + [%s]',
                    \implode(', ', \array_map(fn (Entry $entry) => $entry->name(), $this->entries)),
                    \implode(', ', \array_map(fn (Entry $entry) => $entry->name(), $newEntries)),
                )
            );
        }

        return self::recreate($mergedEntries);
    }

    /**
     * @return Entry[]
     */
    public function all() : array
    {
        return \array_values($this->entries);
    }

    public function count() : int
    {
        return \count($this->entries);
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedArgumentTypeCoercion
     *
     * @param callable(Entry) : bool $callable
     */
    public function filter(callable $callable) : self
    {
        $entries = [];

        foreach ($this->entries as $entry) {
            if ($callable($entry)) {
                $entries[$entry->name()] = $entry;
            }
        }

        return self::recreate($entries);
    }

    /**
     * @throws RuntimeException
     */
    public function get(string $name) : Entry
    {
        $entry = $this->find($name);

        if ($entry === null) {
            throw new InvalidArgumentException("Entry \"{$name}\" does not exist");
        }

        return $entry;
    }

    /**
     * @return \Iterator<string, Entry>
     */
    public function getIterator() : \Iterator
    {
        return new \ArrayIterator($this->all());
    }

    public function has(string $name) : bool
    {
        return \array_key_exists($name, $this->entries);
    }

    public function isEqual(self $entries) : bool
    {
        if ($this->count() !== $entries->count()) {
            return false;
        }

        foreach ($this->entries as $entry) {
            $otherEntry = $entries->find($entry->name());

            if ($otherEntry === null) {
                return false;
            }

            if (!$otherEntry->isEqual($entry)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedReturnTypeCoercion
     * @template ReturnType
     *
     * @param callable(Entry) : ReturnType $callable
     *
     * @return array<int, ReturnType>
     */
    public function map(callable $callable) : array
    {
        $returnValues = [];

        foreach ($this->entries as $entry) {
            $returnValues[] = $callable($entry);
        }

        return $returnValues;
    }

    public function merge(self $entries) : self
    {
        $newEntries = \array_merge($this->entries, $entries->entries);

        if (\count($newEntries) != $this->count() + $entries->count()) {
            throw InvalidArgumentException::because(
                \sprintf(
                    'Merged entries names must be unique, given: [%s] + [%s]',
                    \implode(', ', \array_map(fn (Entry $entry) => $entry->name(), $this->entries)),
                    \implode(', ', \array_map(fn (Entry $entry) => $entry->name(), $entries->all())),
                )
            );
        }

        return self::recreate($newEntries);
    }

    /**
     * @param string $offset
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function offsetExists($offset) : bool
    {
        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_string($offset)) {
            throw new InvalidArgumentException('Entries accepts only string offsets');
        }

        return $this->has($offset);
    }

    /**
     * @param string $offset
     *
     * @throws InvalidArgumentException
     *
     * @return Entry
     */
    public function offsetGet($offset) : Entry
    {
        if ($this->offsetExists($offset)) {
            return $this->get($offset);
        }

        throw new InvalidArgumentException("Entry {$offset} does not exists.");
    }

    public function offsetSet($offset, $value) : self
    {
        throw new RuntimeException('In order to add new rows use Entries::add(Entry $entry) : self');
    }

    /**
     * @param string $offset
     *
     * @throws InvalidArgumentException
     *
     * @return Entries
     * @psalm-suppress ImplementedReturnTypeMismatch
     */
    public function offsetUnset($offset) : self
    {
        throw new RuntimeException('In order to add new rows use Entries::remove(string $name) : self');
    }

    public function remove(string ...$names) : self
    {
        $entries = $this->entries;

        foreach ($names as $name) {
            if ($this->has($name) === false) {
                throw InvalidLogicException::because(\sprintf('Entry "%s" does not exist', $name));
            }

            unset($entries[$name]);
        }

        return self::recreate($entries);
    }

    public function rename(string $currentName, string $newName) : self
    {
        if (!$this->has($currentName)) {
            throw InvalidLogicException::because(\sprintf('Entry "%s" does not exist', $currentName));
        }

        $entries = $this->entries;

        $entry = $this->get($currentName);

        unset($entries[$currentName]);

        $entries[$newName] = $entry->rename($newName);

        return self::recreate($entries);
    }

    public function set(Entry ...$entries) : self
    {
        $newEntries = $this->entries;

        foreach ($entries as $entry) {
            $newEntries[$entry->name()] = $entry;
        }

        return self::recreate($newEntries);
    }

    public function sort() : self
    {
        $entries = \array_values($this->entries);
        \usort($entries, fn (Entry $a, Entry $b) => $a->name() <=> $b->name());

        return new self(...$entries);
    }

    /**
     * @psalm-suppress MissingClosureReturnType
     *
     * @return array<string, mixed>
     */
    public function toArray() : array
    {
        /** @phpstan-ignore-next-line PHPStan knows that array_combine can also return false which is not going to happen here */
        return \array_combine(
            $this->map(fn (Entry $entry) => $entry->name()),
            $this->map(fn (Entry $entry) => $entry->value())
        );
    }

    private function find(string $name) : ?Entry
    {
        if ($this->has($name)) {
            return $this->entries[$name];
        }

        return null;
    }

    /**
     * Internal function used to create entries that are already indexed and validated against duplicates.
     * It comes with a significant performance boost, only to be used inside of this collection.
     *
     * @psalm-pure
     *
     * @param array<string, Entry> $entries
     *
     * @return self
     */
    private static function recreate(array $entries) : self
    {
        $instance = new self();
        $instance->entries = $entries;

        return $instance;
    }
}
