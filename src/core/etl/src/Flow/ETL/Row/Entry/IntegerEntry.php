<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of int|null
 *
 * @implements Entry<T>
 */
final class IntegerEntry implements Entry
{
    use EntryRef;

    private IntegerDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?int $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new IntegerDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): IntegerDefinition
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
        return (
            $this->is($entry->name())
            && $entry instanceof self
            && type_equals($this->type(), $entry->type())
            && $this->value() === $entry->value()
        );
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
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

        return (string) $this->value();
    }

    /**
     * @return Type<int>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?int
    {
        return $this->value;
    }
}
