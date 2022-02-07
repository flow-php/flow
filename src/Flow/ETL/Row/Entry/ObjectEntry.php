<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class ObjectEntry implements Entry
{
    private string $name;

    private object $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, object $value)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->name = $name;
        $this->value = $value;
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function name() : string
    {
        return $this->name;
    }

    public function value() : object
    {
        return $this->value;
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower($this->name) === \mb_strtolower($name);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    /**
     * @psalm-suppress MixedArgument
     *
     * @throws InvalidArgumentException
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && $this->value() === $entry->value();
    }

    public function toString() : string
    {
        return (string) \preg_replace('!\s+!', ' ', \str_replace("\n", '', \print_r($this->value(), true)));
    }
}
