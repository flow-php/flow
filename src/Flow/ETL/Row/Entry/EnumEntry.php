<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\UnitEnum, array{name: string, value: \UnitEnum}>
 * @psalm-immutable
 */
final class EnumEntry implements Entry
{
    public function __construct(
        private readonly string $name,
        private readonly \UnitEnum $value
    ) {
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
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
    }

    public function definition() : Definition
    {
        /** @psalm-suppress ImpureMethodCall */
        return Definition::enum(
            $this->name,
            \get_class($this->value)
        );
    }

    public function is(string $name) : bool
    {
        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $entry instanceof self && $this->value === $entry->value;
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

    public function value() : \UnitEnum
    {
        return $this->value;
    }
}
