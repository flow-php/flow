<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of Uuid|null
 *
 * @implements Entry<T>
 */
final class UuidEntry implements Entry
{
    use EntryRef;

    private UuidDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?Uuid $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new UuidDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    /**
     * @return self<Uuid>
     */
    public static function from(string $name, string $value): self
    {
        return new self($name, Uuid::fromString($value));
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): UuidDefinition
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
        if (!$entry instanceof self || !$this->is($entry->name()) || !type_equals($this->type(), $entry->type())) {
            return false;
        }

        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($thisValue === null || $entryValue === null) {
            return $thisValue === $entryValue;
        }

        return $thisValue->isEqual($entryValue);
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return self<T>
     *
     * @throws InvalidArgumentException
     */
    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->toString();
    }

    /**
     * @return Type<Uuid>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?Uuid
    {
        return $this->value;
    }
}
