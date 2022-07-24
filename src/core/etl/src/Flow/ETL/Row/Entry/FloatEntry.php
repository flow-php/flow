<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<float, array{name: string, value: float, precision: int}>
 * @psalm-immutable
 */
final class FloatEntry implements \Stringable, Entry
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, private readonly float $value, private readonly int $precision = 6)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    public static function from(string $name, float|int|string $value) : self
    {
        if (!\is_numeric($value) || $value != (int) $value) {
            throw InvalidArgumentException::because(\sprintf('Value "%s" can\'t be casted to integer.', $value));
        }

        return new self($name, (float) $value);
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value, 'precision' => $this->precision];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->precision = $data['precision'];
    }

    public function definition() : Definition
    {
        return Definition::float($this->name, false);
    }

    public function is(string $name) : bool
    {
        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name())
            && $entry instanceof self
            && \bccomp((string) $this->value(), (string) $entry->value(), $this->precision) === 0;
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
        return (string) $this->value();
    }

    public function value() : float
    {
        return $this->value;
    }
}
