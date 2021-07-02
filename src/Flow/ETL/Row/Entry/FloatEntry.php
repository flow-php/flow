<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class FloatEntry implements Entry
{
    private string $key;

    private string $name;

    private float $value;

    private int $precision;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, float $value, int $precision = 6)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->value = $value;
        $this->precision = $precision;
    }

    /**
     * @param float|int|string $value
     */
    public static function from(string $name, $value) : self
    {
        if (!\is_numeric($value) || $value != (int) $value) {
            throw InvalidArgumentException::because(\sprintf('Value "%s" can\'t be casted to integer.', $value));
        }

        return new self($name, (float) $value);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function value() : float
    {
        return $this->value;
    }

    public function is(string $name) : bool
    {
        return $this->key === \mb_strtolower($name);
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
        return $this->is($entry->name())
            && $entry instanceof self
            && \bccomp((string) $this->value(), (string) $entry->value(), $this->precision) === 0;
    }
}
