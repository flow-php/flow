<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_null;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<null>
 */
final class NullEntry implements Entry
{
    use EntryRef;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::null($this->name);
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
        return $this->is($entry->name()) && $entry instanceof self;
    }

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

    public function type() : Type
    {
        return type_null();
    }

    public function value()
    {
        return null;
    }
}
