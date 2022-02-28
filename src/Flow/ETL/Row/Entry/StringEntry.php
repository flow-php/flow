<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class StringEntry implements Entry
{
    private string $name;

    private string $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, string $value)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->name = $name;
        $this->value = $value;
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromDateTime(string $name, \DateTimeInterface $dateTime, string $format = \DateTimeInterface::ATOM) : self
    {
        return new self($name, $dateTime->format($format));
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function lowercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtolower($value));
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function uppercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtoupper($value));
    }

    /**
     * @return array{name: string, value: string}
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
     * @param array{name: string, value: string} $data
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
        return $this->is($entry->name()) && $entry instanceof self && $this->value() === $entry->value();
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
        return $this->value();
    }

    public function value() : string
    {
        return $this->value;
    }
}
