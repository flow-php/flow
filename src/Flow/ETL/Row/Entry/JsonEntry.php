<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayWeakComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class JsonEntry implements Entry
{
    private string $key;

    private string $name;

    /**
     * @phpstan-ignore-next-line
     */
    private array $value;

    private bool $object;

    /**
     * @phpstan-ignore-next-line
     */
    public function __construct(string $name, array $value)
    {
        if (empty($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->value = $value;
        $this->object = false;
    }

    /**
     * @psalm-pure
     * @phpstan-ignore-next-line
     */
    public static function object(string $name, array $value) : self
    {
        foreach (\array_keys($value) as $key) {
            if (!\is_string($key)) {
                throw InvalidArgumentException::because('All keys for JsonEntry object must be strings');
            }
        }

        $entry = new self($name, $value);
        $entry->object = true;

        return $entry;
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
        if (empty($this->value) && $this->object) {
            return '{}';
        }

        return \json_encode($this->value, JSON_THROW_ON_ERROR);
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
        return new self($this->name, $mapper($this->value));
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayWeakComparison())->equals($this->value, $entry->value);
    }
}
