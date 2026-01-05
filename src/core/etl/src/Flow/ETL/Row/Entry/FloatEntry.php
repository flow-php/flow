<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\is_type;
use function Flow\Types\DSL\{type_equals, type_optional};
use Brick\Math\BigDecimal;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

/**
 * @implements Entry<?float>
 */
final class FloatEntry implements Entry
{
    use EntryRef;

    private FloatDefinition $definition;

    private readonly ?float $value;

    public function __construct(
        private readonly string $name,
        float|int|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->value = $value !== null ? BigDecimal::of($value)->toFloat() : null;
        $this->definition = new FloatDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : FloatDefinition
    {
        return $this->definition;
    }

    public function duplicate() : static
    {
        return new self($this->name, $this->value, $this->definition->metadata());
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
                && is_type($this->type(), $entry->type());
        }

        return $this->is($entry->name())
            && $entry instanceof self
            && type_equals($this->type(), $entry->type())
            /** @phpstan-ignore-next-line */
            && \bccomp((string) $thisValue, (string) $entryValue) === 0;
    }

    public function map(callable $mapper) : static
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
    public function rename(string $name) : static
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return \number_format($this->value, 6, '.', '');
    }

    public function type() : Type
    {
        return $this->definition->type();
    }

    public function value() : ?float
    {
        return $this->value;
    }

    public function withValue(mixed $value) : static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
