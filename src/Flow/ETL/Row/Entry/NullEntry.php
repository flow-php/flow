<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class NullEntry implements Entry
{
    private string $name;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(string $name)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->name = $name;
    }

    public function __serialize() : array
    {
        return ['name' => $this->name];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * @param array{name: string, value: string} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower($this->name) === \mb_strtolower($name);
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self;
    }

    /**
     * @psalm-suppress MixedArgument
     *
     * @throws InvalidArgumentException
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name);
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
        return new self($name);
    }

    public function toString() : string
    {
        return 'null';
    }

    public function value()
    {
        return null;
    }
}
