<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayWeakComparison;
use Flow\ETL\Row\Entry;
use Webmozart\Assert\Assert;

/**
 * @psalm-immutable
 */
final class ArrayEntry implements Entry
{
    private string $key;

    private string $name;

    /**
     * @phpstan-ignore-next-line
     */
    private array $value;

    /**
     * @phpstan-ignore-next-line
     */
    public function __construct(string $name, array $value)
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
     * @phpstan-ignore-next-line
     */
    public function value() : array
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
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayWeakComparison())->equals($this->value(), $entry->value());
    }
}
