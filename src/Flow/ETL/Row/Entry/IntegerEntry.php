<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class IntegerEntry implements Entry
{
    private string $key;

    private string $name;

    private int $value;

    public function __construct(string $name, int $value)
    {
        if (empty($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->value = $value;
    }

    /**
     * @param float|int|string $value
     */
    public static function from(string $name, $value) : self
    {
        if (!\is_numeric($value) || $value != (int) $value) {
            throw InvalidArgumentException::because(\sprintf('Value "%s" can\'t be casted to integer.', $value));
        }

        return new self($name, (int) $value);
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @psalm-suppress MissingReturnType
     */
    public function value() : int
    {
        return $this->value;
    }

    public function is(string $name) : bool
    {
        return $this->key === \mb_strtolower($name);
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    /**
     * @psalm-suppress MixedArgument
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && $this->value() === $entry->value();
    }
}
