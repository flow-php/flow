<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_float;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\FloatType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?float, float>
 */
final class FloatEntry implements Entry
{
    use EntryRef;

    private readonly FloatType $type;

    private readonly ?float $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, ?float $value, public readonly int $precision = 6)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($precision < 0 && $this->precision >= 15) {
            throw InvalidArgumentException::because('Precision must be greater or equal to 0 and less than 15');
        }

        $this->value = $value ? round($value, $this->precision) : null;
        $this->type = type_float($this->value === null, $this->precision);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::float($this->name, $this->type->nullable(), $this->precision);
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
            && \bccomp((string) $thisValue, (string) $entryValue, $this->precision) === 0;
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

        return \number_format($this->value, $this->precision, '.', '');
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?float
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
