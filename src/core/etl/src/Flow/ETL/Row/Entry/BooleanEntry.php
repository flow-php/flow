<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of bool|null
 *
 * @implements Entry<T>
 */
final class BooleanEntry implements Entry
{
    use EntryRef;

    private BooleanDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?bool $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new BooleanDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): BooleanDefinition
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

        return $this->value() ? 'true' : 'false';
    }

    /**
     * @return Type<bool>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?bool
    {
        return $this->value;
    }
}
