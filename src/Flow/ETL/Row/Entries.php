<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\CollectionEntry;

/**
 * @psalm-immutable
 */
final class Entries implements \Countable
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

    public function has(string $name) : bool
    {
        return $this->find($name) !== null;
    }

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

    public function appendTo(string $name, self $entries) : self
    {
        $entry = $this->get($name);

        if (!$entry instanceof CollectionEntry) {
            throw RuntimeException::because('Entries can be appended only to "%s", "%s" is type of "%s"', CollectionEntry::class, $name, \get_class($entry));
        }

        return $this
            ->remove($name)
            ->add($entry->append($entries));
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
