<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<string, array{name: string, value: array, object: boolean}>
 */
final class JsonEntry implements \Stringable, Entry
{
    use EntryRef;

    private bool $object = false;

    private readonly array $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, array|string $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\is_string($value)) {
            $this->object = \str_starts_with($value, '{') && \str_ends_with($value, '}');

            try {
                $this->value = (array) \json_decode($value, true, flags: \JSON_THROW_ON_ERROR);
            } catch (\JsonException $e) {
                throw new InvalidArgumentException("Invalid value given: '{$value}', reason: " . $e->getMessage(), previous: $e);
            }
        } else {
            $this->value = $value;
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function object(string $name, array $value) : self
    {
        foreach (\array_keys($value) as $key) {
            if (!\is_string($key)) {
                throw InvalidArgumentException::because('All keys for JsonEntry object must be strings');
            }
        }

        $entry = new self($name, $value);
        $entry->object = true;

        return $entry;
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
            'object' => $this->object,
        ];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->object = $data['object'];
    }

    public function definition() : Definition
    {
        return Definition::json($this->name);
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
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value, $entry->value);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function phpType() : Type
    {
        if ([] === $this->value) {
            return ArrayType::empty();
        }

        return new ArrayType();
    }

    public function rename(string $name) : Entry
    {
        $entry = new self($name, $this->value);
        $entry->object = $this->object;

        return $entry;
    }

    public function toString() : string
    {
        return $this->value();
    }

    /**
     * @throws \JsonException
     */
    public function value() : string
    {
        if (!\count($this->value) && $this->object) {
            return '{}';
        }

        return \json_encode($this->value, JSON_THROW_ON_ERROR);
    }
}
