<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<string, array{name: string, value: string}>
 *
 * @psalm-immutable
 */
final class StringEntry implements \Stringable, Entry
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private string $value)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromDateTime(string $name, \DateTimeInterface $dateTime, string $format = \DateTimeInterface::ATOM) : self
    {
        return new self($name, $dateTime->format($format));
    }

    /**
     * @psalm-pure
     *
     * @throws InvalidArgumentException
     */
    public static function lowercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtolower($value));
    }

    /**
     * @psalm-pure
     *
     * @throws InvalidArgumentException
     */
    public static function uppercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtoupper($value));
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
        return Definition::string($this->name, false);
    }

    public function is(string $name) : bool
    {
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
        return $this->value();
    }

    public function value() : string
    {
        return $this->value;
    }
}
