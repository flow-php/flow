<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of DateTimeZone|null
 *
 * @implements Entry<T>
 */
final class TimeZoneEntry implements Entry
{
    use EntryRef;

    private TimeZoneDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?DateTimeZone $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new TimeZoneDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    /**
     * @return self<DateTimeZone>
     */
    public static function from(string $name, string $value): self
    {
        return new self($name, new DateTimeZone($value));
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): TimeZoneDefinition
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

        return $thisValue->getName() === $entryValue->getName();
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

        return $this->value->getName();
    }

    /**
     * @return Type<DateTimeZone>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?DateTimeZone
    {
        return $this->value;
    }
}
