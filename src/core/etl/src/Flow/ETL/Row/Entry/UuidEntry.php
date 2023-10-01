<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<Entry\Type\Uuid, array{name: string, value: string}>
 */
final class UuidEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly Entry\Type\Uuid $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    public static function from(string $name, string $value) : self
    {
        return new self($name, Entry\Type\Uuid::fromString($value));
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value->toString()];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = new Entry\Type\Uuid($data['value']);
    }

    public function definition() : Definition
    {
        return Definition::uuid($this->name);
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
        return $this->is($entry->name()) && $entry instanceof self && $this->value()->isEqual($entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        return $this->value->toString();
    }

    public function value() : Entry\Type\Uuid
    {
        return $this->value;
    }
}
