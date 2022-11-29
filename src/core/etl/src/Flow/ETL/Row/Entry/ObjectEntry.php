<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<object, array{name: string, value: object}>
 *
 * @psalm-immutable
 */
final class ObjectEntry implements \Stringable, Entry
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly object $value)
    {
        if (!\strlen($name)) {
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

    public function is(string $name) : bool
    {
        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        /** @psalm-suppress ImpureFunctionCall */
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
