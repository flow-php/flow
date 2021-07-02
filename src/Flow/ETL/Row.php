<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;

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

    /**
     * @throws RuntimeException
     */
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

    public function remove(string $name) : self
    {
        if (!$this->entries->has($name)) {
            return $this;
        }

        return new self($this->entries->remove($name));
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
