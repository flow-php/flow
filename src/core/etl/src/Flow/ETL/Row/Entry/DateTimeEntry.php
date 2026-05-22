<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of \DateTimeInterface|null
 *
 * @implements Entry<T>
 */
final class DateTimeEntry implements Entry
{
    use EntryRef;

    private DateTimeDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?DateTimeInterface $value,
        ?Metadata $metadata = null,
    ) {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new DateTimeDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): DateTimeDefinition
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

        $thisValue = $this->value();
        $entryValue = $entry->value();

        if ($thisValue === null || $entryValue === null) {
            return $thisValue === $entryValue;
        }

        return $thisValue == $entryValue;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        $value = $this->value;

        if ($value === null) {
            return '';
        }

        return $value->format(DateTimeInterface::ATOM);
    }

    /**
     * @return Type<DateTimeInterface>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?DateTimeInterface
    {
        return $this->value;
    }
}
