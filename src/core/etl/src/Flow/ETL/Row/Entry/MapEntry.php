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
use Flow\Types\Type\TypeDetector;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_optional;

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Entry<?array<TKey, TValue>>
 */
final class MapEntry implements Entry
{
    use EntryRef;

    /**
     * @var MapDefinition<TKey, TValue>
     */
    private MapDefinition $definition;

    /**
     * @param ?array<array-key, mixed> $value
     * @param MapType<TKey, TValue> $type
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?array $value,
        MapType $type,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value !== null && !$type->isValid($value)) {
            throw InvalidArgumentException::because(
                'Expected ' . $type->toString() . ' got different types: ' . (new TypeDetector())
                    ->detectType($this->value)
                    ->toString(),
            );
        }

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

    public function duplicate(): static
    {
        return new self($this->name, $this->value, $this->type(), $this->definition->metadata());
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
        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue !== null) {
            return false;
        }

        if ($entryValue !== null && $thisValue === null) {
            return false;
        }

        if ($entryValue === null && $thisValue === null) {
            return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type(), $entry->type());
        }

        return (
            $this->is($entry->name())
            && $entry instanceof self
            && type_equals($this->type(), $entry->type())
            && (new ArrayComparison())->equals($thisValue, \is_array($entryValue) ? $entryValue : null)
        );
    }

    public function map(callable $mapper): static
    {
        return new self($this->name, $mapper($this->value), $this->type());
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

        return \json_encode($this->value(), JSON_THROW_ON_ERROR);
    }

    /**
     * @return MapType<TKey, TValue>
     */
    public function type(): MapType
    {
        return $this->definition->type();
    }

    public function value(): ?array
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->type());
    }
}
