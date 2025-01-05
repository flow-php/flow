<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?\UnitEnum, \UnitEnum>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    private readonly EnumType $type;

    public function __construct(
        private readonly string $name,
        private readonly ?\UnitEnum $value,
    ) {
        $this->type = EnumType::of($value === null ? \UnitEnum::class : $value::class, $value === null);
    }

    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    public function definition() : Definition
    {
        return Definition::enum(
            $this->name,
            $this->type->class,
            $this->type->nullable()
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
        return $entry instanceof self && $this->type->isEqual($entry->type) && $this->value === $entry->value;
    }

    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\UnitEnum
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
