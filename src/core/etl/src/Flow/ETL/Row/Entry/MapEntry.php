<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\{Type, TypeDetector};
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?array<array-key, mixed>, array<array-key, mixed>>
 */
final class MapEntry implements Entry
{
    use EntryRef;

    private MapType $type;

    /**
     * @param ?array<mixed> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?array $value,
        MapType $type,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (!$type->isValid($value)) {
            throw InvalidArgumentException::because('Expected ' . $type->toString() . ' got different types: ' . (new TypeDetector())->detectType($this->value)->toString());
        }

        $this->type = $type->makeNullable($this->value === null);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::map(
            $this->name,
            $this->type,
        );
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
                && $this->type->isEqual($entry->type);
        }

        return $this->is($entry->name())
            && $entry instanceof self
            && $this->type->isEqual($entry->type)
            && (new ArrayComparison())->equals($thisValue, $entryValue);
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
        return new self($this->name, $value, $this->type);
    }
}
