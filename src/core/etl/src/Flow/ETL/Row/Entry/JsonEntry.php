<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<string, array{name: string, value: array<mixed>, object: boolean}>
 */
final class JsonEntry implements \Stringable, Entry
{
    use EntryRef;

    private bool $object;

    /**
     * JsonEntry constructor.
     *
     * @param array<mixed> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly array $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->object = false;
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public static function fromJsonString(string $name, string $json) : self
    {
        if (\str_starts_with($json, '{') && \str_ends_with($json, '}')) {
            return self::object($name, (array) \json_decode($json, true, 515, JSON_THROW_ON_ERROR));
        }

        return new self($name, (array) \json_decode($json, true, 515, JSON_THROW_ON_ERROR));
    }

    /**
     * @param array<mixed> $value
     *
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
        return Definition::json($this->name, false);
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
        return self::fromJsonString($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
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

    public function value() : string
    {
        if (empty($this->value) && $this->object) {
            return '{}';
        }

        return \json_encode($this->value, JSON_THROW_ON_ERROR);
    }
}
