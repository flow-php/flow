<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_boolean, type_equals, type_optional};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?bool>
 */
final class BooleanEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<bool>
     */
    private readonly Type $type;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly ?bool $value, ?Metadata $metadata = null)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_boolean();
    }

    #[\Override]
    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * @return Definition<bool>
     */
    #[\Override]
    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    #[\Override]
    public function duplicate() : Entry
    {
        return new self($this->name, $this->value, $this->metadata);
    }

    #[\Override]
    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    #[\Override]
    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type, $entry->type) && $this->value() === $entry->value();
    }

    #[\Override]
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    #[\Override]
    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    #[\Override]
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    #[\Override]
    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value() ? 'true' : 'false';
    }

    #[\Override]
    public function type() : Type
    {
        return $this->type;
    }

    #[\Override]
    public function value() : ?bool
    {
        return $this->value;
    }

    #[\Override]
    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
