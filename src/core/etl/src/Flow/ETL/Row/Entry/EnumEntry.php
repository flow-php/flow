<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\UnitEnum, array{name: string, value: \UnitEnum, type: EnumType}>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    private readonly EnumType $type;

    public function __construct(
        private readonly string $name,
        private readonly \UnitEnum $value
    ) {
        $this->type = EnumType::of($value::class);
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
            'type' => $this->type,
        ];
    }

    public function __toString() : string
    {
        return $this->value->name;
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->type = $data['type'];
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
        return $this->value->name;
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : \UnitEnum
    {
        return $this->value;
    }
}
