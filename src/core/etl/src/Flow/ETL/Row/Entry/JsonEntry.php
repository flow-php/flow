<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_json, type_optional};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?string>
 */
final class JsonEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    private bool $object = false;

    /**
     * @var Type<string>
     */
    private readonly Type $type;

    /**
     * @var null|string
     */
    private readonly ?string $value;

    /**
     * @param null|string $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        string|null $value,
        ?Metadata $metadata = null,
    ) {
        if ($value !== null && !type_json()->isValid($value)) {
            throw new InvalidArgumentException("Invalid JSON value given: '{$value}'");
        }

        if ($value !== null) {
            $this->object = \str_starts_with($value, '{') && \str_ends_with($value, '}');
        }

        $this->value = $value;
        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_json();
    }

    /**
     * @param null|array<string, mixed> $value
     *
     * @throws InvalidArgumentException
     * @throws \JsonException
     *
     * @return Entry<?string>
     */
    public static function object(string $name, ?array $value, ?Metadata $metadata = null) : Entry
    {
        if ($value === null) {
            return new self($name, null, $metadata);
        }

        foreach (\array_keys($value) as $key) {
            if (!\is_string($key)) {
                throw InvalidArgumentException::because('All keys for JsonEntry object must be strings');
            }
        }

        $json = \json_encode($value, \JSON_THROW_ON_ERROR);
        $entry = new self($name, $json, $metadata);
        $entry->object = true;

        return $entry;
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * @return Definition<string>
     *
     * @phpstan-ignore-next-line
     */
    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : Entry
    {
        $entry = new self($this->name, $this->value, $this->metadata);
        $entry->object = $this->object;

        return $entry;
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
        $entryValue = $entry instanceof self ? $entry->value : $entry->value();
        $thisValue = $this->value;

        if ($entryValue === null && $thisValue !== null) {
            return false;
        }

        if ($entryValue !== null && $thisValue === null) {
            return false;
        }

        if ($entryValue === null && $thisValue === null) {
            return $this->is($entry->name())
                && $entry instanceof self
                && type_equals($this->type, $entry->type);
        }

        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type, $entry->type) && $thisValue === $entryValue;
    }

    public function map(callable $mapper) : Entry
    {
        $entry = new self($this->name, $mapper($this->value()), $this->metadata);
        $entry->object = $this->object;

        return $entry;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        $entry = new self($name, $this->value, $this->metadata);
        $entry->object = $this->object;

        return $entry;
    }

    public function toString() : string
    {
        return $this->value ?? '';
    }

    /**
     * @return Type<string>
     *
     * @phpstan-ignore-next-line
     */
    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?string
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
