<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\TypeDetector;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_optional;
use function json_encode;

/**
 * @template T
 *
 * @implements Entry<?list<T>>
 */
final class ListEntry implements Entry
{
    use EntryRef;

    /**
     * @var ?list<T>
     */
    private readonly ?array $value;

    /**
     * @var ListDefinition<T>
     */
    private ListDefinition $definition;

    /**
     * @param ListType<T> $type
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        mixed $value,
        ListType $type,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value !== null && !$type->isValid($value)) {
            throw InvalidArgumentException::because(
                'Expected ' . $type->toString() . ' got different types: ' . (new TypeDetector())
                    ->detectType($value)
                    ->toString(),
            );
        }

        $this->value = $value;

        $this->definition = new ListDefinition(
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
     * @return ListDefinition<T>
     */
    public function definition(): ListDefinition
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

    public function map(callable $mapper): static
    {
        return new self($this->name, type_optional($this->type())->assert($mapper($this->value)), $this->type());
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
     * @return ListType<T>
     */
    public function type(): ListType
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
