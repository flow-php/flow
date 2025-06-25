<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_optional};
use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Type\{TypeDetector};

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Entry<?array<TKey, TValue>>
 */
final class MapEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<array<TKey, TValue>>
     */
    private Type $type;

    /**
     * @param ?array<array-key, mixed> $value
     * @param Type<array<TKey, TValue>> $type
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?array $value,
        Type $type,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value !== null && !$type->isValid($value)) {
            throw InvalidArgumentException::because('Expected ' . $type->toString() . ' got different types: ' . (new TypeDetector())->detectType($this->value)->toString());
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = $type;
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : Entry
    {
        return new self($this->name, $this->value, $this->type, $this->metadata);
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
        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue !== null) {
            return false;
        }

        if ($entryValue !== null && $thisValue === null) {
            return false;
        }

        if ($entryValue === null && $thisValue === null) {
            return $this->is($entry->name())
                && $entry instanceof self
                && type_equals($this->type, $entry->type);
        }

        return $this->is($entry->name())
            && $entry instanceof self
            && type_equals($this->type, $entry->type)
            && (new ArrayComparison())->equals($thisValue, \is_array($entryValue) ? $entryValue : null);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value), $this->type);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value, $this->type);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return \json_encode($this->value(), JSON_THROW_ON_ERROR);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?array
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->type);
    }
}
