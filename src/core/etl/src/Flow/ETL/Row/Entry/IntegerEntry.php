<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_int;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?int>
 */
final class IntegerEntry implements Entry
{
    use EntryRef;

    private readonly ScalarType $type;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly ?int $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->type = type_int($this->value === null);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::integer($this->name, $this->type->nullable());
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
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && $this->value() === $entry->value();
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return (string) $this->value();
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?int
    {
        return $this->value;
    }
}
