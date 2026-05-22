<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function bccomp;
use function Flow\ETL\DSL\is_type;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_numeric_string;
use function number_format;
use function sprintf;

/**
 * @template-covariant T of float|null
 *
 * @implements Entry<T>
 */
final class FloatEntry implements Entry
{
    use EntryRef;

    private FloatDefinition $definition;

    /**
     * @param T $value
     */
    public function __construct(
        private readonly string $name,
        private readonly ?float $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new FloatDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): FloatDefinition
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
        if (!$entry instanceof self || !$this->is($entry->name())) {
            return false;
        }

        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue === null) {
            return is_type($this->type(), $entry->type());
        }

        if ($entryValue === null || $thisValue === null) {
            return false;
        }

        return (
            type_equals($this->type(), $entry->type())
            && bccomp(
                type_numeric_string()->assert(sprintf('%.20F', $thisValue)),
                type_numeric_string()->assert(sprintf('%.20F', $entryValue)),
            ) === 0
        );
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
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

        return number_format($this->value, 6, '.', '');
    }

    /**
     * @return Type<float>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?float
    {
        return $this->value;
    }
}
