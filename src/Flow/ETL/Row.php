<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Converter;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\CollectionEntry;

/**
 * @psalm-immutable
 */
final class Row
{
    private Entries $entries;

    public function __construct(Entries $entries)
    {
        $this->entries = $entries;
    }

    /**
     * @psalm-pure
     */
    public static function create(Entry ...$entries) : self
    {
        return new self(new Entries(...$entries));
    }

    public function entries() : Entries
    {
        return $this->entries;
    }

    public function get(string $name) : Entry
    {
        return $this->entries->get($name);
    }

    /**
     * @psalm-suppress MissingReturnType
     * @phpstan-ignore-next-line
     */
    public function valueOf(string $name)
    {
        return $this->get($name)->value();
    }

    public function set(Entry $entry) : self
    {
        return new self($this->entries->set($entry));
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedArgumentTypeCoercion
     */
    public function map(callable $mapper) : self
    {
        return new self(new Entries(...$this->entries->map($mapper)));
    }

    public function rename(string $currentName, string $newName) : self
    {
        return new self(
            $this->entries
                ->remove($currentName)
                ->add($this->entries->get($currentName)->rename($newName))
        );
    }

    public function pullOutFrom(string $collectionName, string $entryName) : self
    {
        $collection = $this->get($collectionName);

        if (!$collection instanceof CollectionEntry) {
            throw RuntimeException::because('Entry can be pulled out only from "%s", but "%s" is a "%s"', CollectionEntry::class, $collectionName, \get_class($collection));
        }

        return $this
            ->set($collection->entryFromAll($entryName))
            ->set($collection->removeFromAll($entryName));
    }

    public function convert(string $name, Converter $serializer) : self
    {
        return $this->set(
            $serializer->convert($this->get($name))
        );
    }

    /**
     * @param callable(Entry) : bool $callable
     */
    public function filter(callable $callable) : self
    {
        return new self($this->entries->filter($callable));
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray() : array
    {
        return $this->entries->toArray();
    }

    public function isEqual(self $row) : bool
    {
        return $this->entries->isEqual($row->entries());
    }

    public function add(Entry $entry) : self
    {
        return new self($this->entries->add($entry));
    }

    public function sortEntries() : self
    {
        return new self($this->entries->sort());
    }
}
