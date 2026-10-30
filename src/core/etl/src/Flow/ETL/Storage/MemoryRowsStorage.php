<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_key_exists;

final class MemoryRowsStorage implements Describable, ResidentRowsStorage
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    /**
     * A worker's memory storage is its own, empty.
     */
    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    /**
     * @var array<string, list<Rows>>
     */
    private array $entries = [];

    public function append(string $key, Rows $rows): void
    {
        $this->entries[$key] ??= [];

        if (!$rows->isEmpty()) {
            $this->entries[$key][] = $rows;
        }
    }

    public function close(string $key): void {}

    public function get(string $key): Generator
    {
        foreach ($this->entries[$key] ?? [] as $batch) {
            yield $batch;
        }
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->entries);
    }

    public function remove(string $key): void
    {
        unset($this->entries[$key]);
    }

    public function rows(string $key): Cardinality
    {
        $rows = 0;

        foreach ($this->entries[$key] ?? [] as $batch) {
            $rows += $batch->count();
        }

        return Cardinality::exact($rows);
    }

    public function schema(string $key): ?Schema
    {
        if (!array_key_exists($key, $this->entries)) {
            return null;
        }

        $schema = new Schema();

        foreach ($this->entries[$key] as $batch) {
            $schema = $schema->merge($batch->schema());
        }

        return $schema;
    }
}
