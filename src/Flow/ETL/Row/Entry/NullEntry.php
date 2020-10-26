<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Webmozart\Assert\Assert;

/**
 * @psalm-immutable
 */
final class NullEntry implements Entry
{
    private string $key;

    private string $name;

    public function __construct(string $name)
    {
        Assert::notEmpty($name, 'Entry name cannot be empty');

        $this->key = \mb_strtolower($name);
        $this->name = $name;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @psalm-suppress MissingReturnType
     * @phpstan-ignore-next-line
     */
    public function value()
    {
        return null;
    }

    public function is(string $name) : bool
    {
        return $this->key === \mb_strtolower($name);
    }

    public function rename(string $name) : Entry
    {
        return new self($name);
    }

    /**
     * @psalm-suppress MixedArgument
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name);
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self;
    }
}
