<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Value\Json;

use function array_keys;
use function Flow\Types\DSL\type_equals;
use function is_string;

/**
 * @template-covariant T of Json|null
 *
 * @implements Entry<T>
 */
final class JsonEntry implements Entry
{
    use EntryRef;

    private JsonDefinition $definition;

    /**
     * @param T $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?Json $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new JsonDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    /**
     * @param null|array<array-key, mixed> $value
     *
     * @throws InvalidArgumentException
     *
     * @return ($value is null ? Entry<null> : Entry<Json>)
     */
    public static function object(string $name, ?array $value, ?Metadata $metadata = null): Entry
    {
        if (is_array($value)) {
            foreach (array_keys($value) as $key) {
                if (!is_string($key)) {
                    throw InvalidArgumentException::because('All keys for JsonEntry object must be strings');
                }
            }
        }

        if ($value === null) {
            return new self($name, null, $metadata);
        }

        return new self($name, Json::fromArray($value, asObject: true), $metadata);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): JsonDefinition
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
        if (!$entry instanceof self) {
            return false;
        }

        if (!$this->is($entry->name())) {
            return false;
        }

        if (!type_equals($this->type(), $entry->type())) {
            return false;
        }

        $thisJson = $this->value;
        $entryJson = $entry->value;

        if ($thisJson === null && $entryJson === null) {
            return true;
        }

        if ($thisJson === null || $entryJson === null) {
            return false;
        }

        return $thisJson->isEqual($entryJson);
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return self<T>
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

        return $this->value->toString();
    }

    /**
     * @return Type<Json>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?Json
    {
        return $this->value;
    }
}
