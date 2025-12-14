<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_json, type_optional};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Value\Json;

/**
 * @implements Entry<?Json>
 */
final class JsonEntry implements Entry
{
    use EntryRef;

    private readonly ?Json $json;

    private Metadata $metadata;

    /**
     * @var Type<Json>
     */
    private readonly Type $type;

    /**
     * @param null|array<array-key, mixed>|Json|string $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        array|string|Json|null $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if ($value instanceof Json) {
            $this->json = $value;
        } elseif (\is_string($value)) {
            try {
                $this->json = new Json($value);
            } catch (\Throwable $e) {
                throw new InvalidArgumentException("Invalid value given: '{$value}', reason: " . $e->getMessage(), previous: $e);
            }
        } elseif (\is_array($value)) {
            $this->json = Json::fromArray($value);
        } else {
            $this->json = null;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_json();
    }

    /**
     * @param null|array<array-key, mixed> $value
     *
     * @throws InvalidArgumentException
     *
     * @return Entry<?Json>
     */
    public static function object(string $name, ?array $value, ?Metadata $metadata = null) : Entry
    {
        if (\is_array($value)) {
            foreach (\array_keys($value) as $key) {
                if (!\is_string($key)) {
                    throw InvalidArgumentException::because('All keys for JsonEntry object must be strings');
                }
            }
        }

        if ($value === null) {
            return new self($name, null, $metadata);
        }

        return new self($name, Json::fromArray($value, asObject: true), $metadata);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : JsonDefinition
    {
        return new JsonDefinition($this->name, $this->json === null, $this->metadata);
    }

    public function duplicate() : static
    {
        return new self($this->name, $this->json, $this->metadata);
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
        if (!$entry instanceof self) {
            return false;
        }

        if (!$this->is($entry->name())) {
            return false;
        }

        if (!type_equals($this->type, $entry->type)) {
            return false;
        }

        $thisJson = $this->json;
        $entryJson = $entry->json;

        if ($thisJson === null && $entryJson === null) {
            return true;
        }

        if ($thisJson === null || $entryJson === null) {
            return false;
        }

        return $thisJson->isEqual($entryJson);
    }

    public function map(callable $mapper) : static
    {
        return new self($this->name, $mapper($this->json), $this->metadata);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : static
    {
        return new self($name, $this->json, $this->metadata);
    }

    public function toString() : string
    {
        if ($this->json === null) {
            return '';
        }

        return $this->json->toString();
    }

    /**
     * @return Type<Json>
     */
    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?Json
    {
        return $this->json;
    }

    public function withValue(mixed $value) : static
    {
        return new self($this->name, type_optional($this->type())->cast($value), $this->metadata);
    }
}
