<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\MapType;

use function Flow\Types\DSL\type_equals;
use function json_encode;

/**
 * @template TKey of array-key
 * @template TValue
 * @template-covariant TMap of array<TKey, TValue>|null
 *
 * @implements Entry<TMap>
 */
final class MapEntry implements Entry
{
    use EntryRef;

    /**
     * @var TMap
     */
    private readonly ?array $value;

    /**
     * @var MapDefinition<TKey, TValue>
     */
    private MapDefinition $definition;

    /**
     * @param TMap $value
     * @param MapType<array<TKey, TValue>> $type
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        mixed $value,
        MapType $type,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->value = $value;

        $this->definition = new MapDefinition(
            $this->name,
            $type,
            $this->value === null,
            $metadata ?: Metadata::empty(),
        );
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    /**
     * @return MapDefinition<TKey, TValue>
     */
    public function definition(): MapDefinition
    {
        return $this->definition;
    }

    public function is(string|Reference $name): bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry): bool
    {
        if (!$entry instanceof self) {
            return false;
        }

        if (!$this->is($entry->name()) || !type_equals($this->type(), $entry->type())) {
            return false;
        }

        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null || $thisValue === null) {
            return $entryValue === $thisValue;
        }

        return (new ArrayComparison())->equals($thisValue, $entryValue);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->type(), $this->definition->metadata());
    }

    public function toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return json_encode($this->value(), JSON_THROW_ON_ERROR);
    }

    /**
     * @return MapType<array<TKey, TValue>>
     */
    public function type(): MapType
    {
        return $this->definition->type();
    }

    /**
     * @return TMap
     */
    public function value(): ?array
    {
        return $this->value;
    }
}
