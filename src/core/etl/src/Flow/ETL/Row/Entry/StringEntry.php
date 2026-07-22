<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;
use function mb_strtolower;
use function mb_strtoupper;

/**
 * @template-covariant T of string|null
 *
 * @implements Entry<T>
 */
final class StringEntry implements Entry
{
    use EntryRef;

    private StringDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?string $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new StringDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    /**
     * @return self<string>
     *
     * @throws InvalidArgumentException
     */
    public static function lowercase(string $name, string $value): self
    {
        return new self($name, mb_strtolower($value));
    }

    /**
     * @return self<string>
     *
     * @throws InvalidArgumentException
     */
    public static function uppercase(string $name, string $value): self
    {
        return new self($name, mb_strtoupper($value));
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): StringDefinition
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
        return (
            $this->is($entry->name())
            && $entry instanceof self
            && type_equals($this->type(), $entry->type())
            && $this->value() === $entry->value()
        );
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return self<T>
     *
     * @throws InvalidArgumentException
     */
    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    /**
     * @return self<string|null>
     */
    public function toLowercase(): self
    {
        return new self($this->name, $this->value ? mb_strtolower($this->value) : null);
    }

    public function toString(): string
    {
        $value = $this->value();

        if ($value === null) {
            return '';
        }

        return $value;
    }

    /**
     * @return Type<string>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?string
    {
        return $this->value;
    }
}
