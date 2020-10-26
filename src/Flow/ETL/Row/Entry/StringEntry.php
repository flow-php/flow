<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Webmozart\Assert\Assert;

/**
 * @psalm-immutable
 */
final class StringEntry implements Entry
{
    private string $key;

    private string $name;

    private string $value;

    public function __construct(string $name, string $value)
    {
        Assert::notEmpty($name, 'Entry name cannot be empty');

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->value = $value;
    }

    public static function lowercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtolower($value));
    }

    public static function uppercase(string $name, string $value) : self
    {
        return new self($name, \mb_strtoupper($value));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @psalm-suppress MissingReturnType
     */
    public function value() : string
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
