<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<object, array{name: string, value: object}>
 */
final class ObjectEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly object $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
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
        return Definition::object($this->name, false);
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
        return $this->is($entry->name())
            && $entry instanceof self
            && \serialize($this->__serialize()['value']) === \serialize($entry->__serialize()['value']);
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
        return new ObjectType(\get_class($this->value), false);
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
        return (string) \preg_replace('!\s+!', ' ', \str_replace("\n", '', \print_r($this->value(), true)));
    }

    public function value() : object
    {
        return $this->value;
    }
}
