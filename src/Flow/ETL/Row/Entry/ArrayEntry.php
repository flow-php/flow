<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class ArrayEntry implements Entry
{
    private string $name;

    /**
     * @var array<mixed>
     */
    private array $value;

    /**
     * @param string $name
     * @param array<mixed> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, array $value)
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

    /**
     * @return array<mixed>
     */
    public function value() : array
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
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value());
    }

    public function toString() : string
    {
        return (string) \preg_replace('!\s+!', ' ', \str_replace("\n", '', \print_r($this->value(), true)));
    }
}
