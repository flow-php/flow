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

    /**
     * @return array{name: string, value: array<mixed>}
     */
    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * @param array{name: string, value: array<mixed>} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower($this->name) === \mb_strtolower($name);
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value());
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
        return (string) \json_encode($this->value());
    }

    /**
     * @return array<mixed>
     */
    public function value() : array
    {
        return $this->value;
    }
}
