<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<int, array{name: string, value: integer}>
 */
final class IntegerEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly int $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    public static function from(string $name, float|int|string $value) : self
    {
        if (!\is_numeric($value) || $value != (int) $value) {
            throw InvalidArgumentException::because(\sprintf('Value "%s" can\'t be casted to integer.', $value));
        }

        return new self($name, (int) $value);
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
    }

    public function definition() : Definition
    {
        return Definition::integer($this->name, false);
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
        return $this->is($entry->name()) && $entry instanceof self && $this->value() === $entry->value();
    }

    public function map(callable $mapper) : Entry
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
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        return (string) $this->value();
    }

    public function value() : int
    {
        return $this->value;
    }
}
