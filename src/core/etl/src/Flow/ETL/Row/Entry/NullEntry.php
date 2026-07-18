<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

/**
 * @implements Entry<null>
 */
final class NullEntry implements Entry
{
    use EntryRef;

    private readonly null $value;

    private NullDefinition $definition;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->value = null;
        $this->definition = new NullDefinition($this->name, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): NullDefinition
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
        return $this->is($entry->name()) && $entry instanceof self;
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
        return new self($name, $this->definition->metadata());
    }

    public function toString(): string
    {
        return '';
    }

    /**
     * @return Type<null>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    public function value(): null
    {
        return $this->value;
    }
}
