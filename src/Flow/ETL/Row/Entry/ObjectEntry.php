<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Webmozart\Assert\Assert;

/**
 * @psalm-immutable
 */
final class ObjectEntry implements Entry
{
    private string $key;

    private string $name;

    private object $value;

    public function __construct(string $name, object $value)
    {
        Assert::notEmpty($name, 'Entry name cannot be empty');

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->value = $value;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @psalm-suppress MissingReturnType
     */
    public function value() : object
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
