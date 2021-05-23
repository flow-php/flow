<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\RuntimeException;

/**
 * @implements \ArrayAccess<string, Entry>
 * @implements \IteratorAggregate<string, Entry>
 * @psalm-immutable
 */
final class Entries  implements \ArrayAccess, \Countable, \IteratorAggregate
{
    /**
     * @var Entry[]
     */
    private array $entries;

    public function __construct(Entry ...$entries)
    {
        $names = \array_map(fn (Entry $entry) => $entry->name(), $entries);

        if (\count($names) !== \count(\array_unique($names))) {
            throw InvalidArgumentException::because(\sprintf('Entry names must be unique, given: [%s]', \implode(', ', $names)));
        }

        $this->entries = $entries;
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

    /**
     * @return \Iterator<string, Entry>
     */
    public function getIterator() : \Iterator
    {
        return new \ArrayIterator($this->entries);
    }

    public function has(string $name) : bool
    {
        return $this->find($name) !== null;
    }

    /**
     * @throws RuntimeException
     */
    public function get(string $name) : Entry
    {
        $entry = $this->find($name);

        if ($entry === null) {
            throw RuntimeException::because('Entry "%s" does not exist', $name);
        }

        return $entry;
    }

    public function add(Entry $entry) : self
    {
        if ($this->has($entry->name()) === true) {
            throw InvalidLogicException::because(\sprintf('Entry "%s" already exist', $entry->name()));
        }

        return new self(...[...$this->entries, $entry]);
    }

    public function remove(string $name) : self
    {
        if ($this->has($name) === false) {
            throw InvalidLogicException::because(\sprintf('Entry "%s" does not exist', $name));
        }

        return $this->filter(
            fn (Entry $entry) : bool => !$entry->is($name)
        );
    }

    public function set(Entry $entry) : self
    {
        if ($this->has($entry->name())) {
            return $this
                ->remove($entry->name())
                ->add($entry);
        }

        return $this->add($entry);
    }

    public function sort() : self
    {
        $entries = $this->entries;
        \usort($entries, fn (Entry $a, Entry $b) => $a->name() <=> $b->name());

        return new self(...$entries);
    }

    public function count() : int
    {
        return \count($this->entries);
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
        return \array_map($callable, $this->entries);
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedArgumentTypeCoercion
     *
     * @param callable(Entry) : bool $callable
     */
    public function filter(callable $callable) : self
    {
        return new self(...\array_filter($this->entries, $callable));
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
     * @return Entry[]
     */
    public function all() : array
    {
        return $this->entries;
    }

    private function find(string $name) : ?Entry
    {
        foreach ($this->entries as $entry) {
            if ($entry->is($name)) {
                return $entry;
            }
        }

        return null;
    }
}
