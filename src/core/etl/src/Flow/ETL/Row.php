<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;

final class Row
{
    private ?Schema $schema = null;

    public function __construct(
        private readonly Entries $entries,
    ) {}

    /**
     * @param Entry<mixed> ...$entries
     *
     * @throws InvalidArgumentException
     */
    public static function create(Entry ...$entries): self
    {
        return new self(new Entries(...$entries));
    }

    /**
     * @param Entry<mixed> ...$entries
     */
    public static function with(Entry ...$entries): self
    {
        return self::create(...$entries);
    }

    /**
     * @param Entry<mixed> ...$entries
     *
     * @throws InvalidArgumentException
     */
    public function add(Entry ...$entries): self
    {
        return new self($this->entries->add(...$entries));
    }

    public function entries(): Entries
    {
        return $this->entries;
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return Entry<mixed>
     */
    public function get(string|Reference $reference): Entry
    {
        return $this->entries->get($reference);
    }

    public function has(string|Reference $reference): bool
    {
        return $this->entries->has($reference);
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()): string
    {
        $string = '';

        foreach ($this->entries->sort()->all() as $entry) {
            $string .= $entry->name() . $entry->toString();
        }

        return $algorithm->hash($string);
    }

    public function isEqual(self $row): bool
    {
        return $this->entries->isEqual($row->entries());
    }

    public function keep(string|Reference ...$references): self
    {
        $entries = [];

        foreach ($references as $name) {
            $entries[] = $this->entries->get($name);
        }

        return new self(new Entries(...$entries));
    }

    /**
     * @param callable(Entry<mixed>) : Entry<mixed> $mapper
     */
    public function map(callable $mapper): self
    {
        return new self(new Entries(...$this->entries->map($mapper)));
    }

    /**
     * @throws InvalidArgumentException
     */
    public function merge(self $row, string $prefix = '_'): self
    {
        return new self(
            $this->entries()->merge(
                $row->map(static fn(Entry $entry): Entry => $entry->rename($prefix . $entry->name()))->entries(),
            ),
        );
    }

    public function remove(string|Reference ...$references): self
    {
        $namesToRemove = [];

        foreach ($references as $name) {
            if ($this->entries->has($name)) {
                $namesToRemove[] = $name;
            }
        }

        return new self($this->entries->remove(...$namesToRemove));
    }

    public function rename(string $currentName, string $newName): self
    {
        return new self($this->entries->rename($currentName, $newName));
    }

    /**
     * Rename multiple entries in a single pass.
     *
     * @param array<string, string> $renames Map of old_name => new_name
     */
    public function renameMany(array $renames): self
    {
        if ($renames === []) {
            return $this;
        }

        return new self($this->entries->renameMany($renames));
    }

    /**
     * @return Schema
     */
    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $definitions = [];

        foreach ($this->entries->all() as $entry) {
            $definitions[] = $entry->definition();
        }

        $this->schema = new Schema(...$definitions);

        return $this->schema;
    }

    /**
     * @param Entry<mixed> ...$entries
     */
    public function set(Entry ...$entries): self
    {
        return new self($this->entries->set(...$entries));
    }

    public function sortEntries(): self
    {
        return new self($this->entries->sort());
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $withKeys = true): array
    {
        return $this->entries->toArray($withKeys);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function valueOf(string|Reference $references): mixed
    {
        // @mago-ignore analysis:mixed-return-statement
        return $this->get($references)->value();
    }
}
