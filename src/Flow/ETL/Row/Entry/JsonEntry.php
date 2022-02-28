<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class JsonEntry implements Entry
{
    private string $name;

    private bool $object;

    /**
     * @var array<mixed>
     */
    private array $value;

    /**
     * JsonEntry constructor.
     *
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
        $this->object = false;
    }

    /**
     * @psalm-pure
     *
     * @param string $name
     * @param array<mixed> $value
     *
     * @throws InvalidArgumentException
     *
     * @return JsonEntry
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

    /**
     * @return array{name: string, value: array<mixed>, object: boolean}
     */
    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
            'object' => $this->object,
        ];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{name: string, value: array<mixed>, object: boolean} $data
     */
    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->object = $data['object'];
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower($this->name) === \mb_strtolower($name);
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value, $entry->value);
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress MoreSpecificImplementedParamType
     * @psalm-param pure-callable $mapper
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        return $this->value();
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
}
