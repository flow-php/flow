<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_optional, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?string>
 */
final class StringEntry implements Entry
{
    use EntryRef;

    private bool $fromNull = false;

    private Metadata $metadata;

    /**
     * @var Type<string>
     */
    private readonly Type $type;

    /**
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

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_string();
    }

    public static function fromNull(string $name, ?Metadata $metadata = null) : self
    {
        $entry = new self($name, null, $metadata);
        $entry->fromNull = true;

        return $entry;
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function lowercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtolower($value));
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function uppercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtoupper($value));
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return new Definition(
            $this->name,
            $this->type,
            $this->value === null,
            $this->fromNull
                ? $this->metadata->merge(Metadata::fromArray([Metadata::FROM_NULL => true]))
                : $this->metadata
        );
    }

    public function duplicate() : self
    {
        return new self($this->name, $this->value, $this->metadata);
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
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type, $entry->type) && $this->value() === $entry->value();
    }

    public function map(callable $mapper) : self
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
    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    public function toLowercase() : self
    {
        return new self($this->name, $this->value ? \mb_strtolower($this->value) : null);
    }

    public function toString() : string
    {
        $value = $this->value();

        if ($value === null) {
            return '';
        }

        return $value;
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?string
    {
        return $this->value;
    }

    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
