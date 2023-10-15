<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<array<array-key, Entry>, array{name: string, entries: array<Entry>}>
 */
final class StructureEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @var array<Entry>
     */
    private readonly array $entries;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, Entry ...$entries)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (!\count($entries)) {
            throw InvalidArgumentException::because('Structure must have at least one entry, ' . $name . ' got none.');
        }

        $entryNames = \array_map(static fn (Entry $entry) => $entry->name(), $entries);

        if (\count(\array_unique($entryNames)) !== \count($entryNames)) {
            throw InvalidArgumentException::because('Each entry name in structure must be unique, given: ' . \implode(', ', $entryNames));
        }

        $this->entries = $entries;
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'entries' => $this->entries,
        ];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->entries = $data['entries'];
    }

    /**
     * @psalm-suppress MixedFunctionCall
     * @psalm-suppress MixedArgumentTypeCoercion
     * @psalm-suppress MixedAssignment
     */
    public function definition() : Definition
    {
        /**
         * @param array<string, Entry|mixed> $entries
         *
         * @return array<string, array<Entry|string>|Entry>
         */
        $buildDefinitions = static function (array $entries) use (&$buildDefinitions) : array {
            $definitions = [];

            /** @var Entry $entry */
            foreach ($entries as $entry) {
                if ($entry instanceof self) {
                    $definitions[$entry->name()] = $buildDefinitions($entry->entries());
                } else {
                    $definitions[$entry->name()] = $entry->definition();
                }
            }

            return $definitions;
        };

        return Definition::structure($this->name, $buildDefinitions($this->entries), false);
    }

    /**
     * @return array<Entry>
     */
    public function entries() : array
    {
        return $this->entries;
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, ...$mapper($this->entries));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, ...$this->entries);
    }

    public function toString() : string
    {
        $array = [];

        foreach ($this->entries as $entry) {
            $array[$entry->name()] = $entry->toString();
        }

        return \json_encode($array, JSON_THROW_ON_ERROR);
    }

    /**
     * @psalm-suppress LessSpecificImplementedReturnType
     * @psalm-suppress MixedAssignment
     *
     * @return array<string, mixed>
     */
    public function value() : array
    {
        /** @var array<string, mixed> $structure */
        $structure = [];

        /** @var Entry $entry */
        foreach ($this->entries as $entry) {
            $structure[$entry->name()] = $entry->value();
        }

        return $structure;
    }
}
