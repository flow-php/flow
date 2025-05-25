<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_date, type_equals};
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateType;

/**
 * @implements Entry<?DateTimeInterface, DateTimeInterface>
 */
final class DateEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    private readonly DateType $type;

    private readonly ?\DateTimeInterface $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, \DateTimeInterface|string|null $value, ?Metadata $metadata = null)
    {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\is_string($value)) {
            try {
                $this->value = (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0);
            } catch (\Exception $e) {
                throw new InvalidArgumentException("Invalid value given: '{$value}', reason: " . $e->getMessage(), previous: $e);
            }
        } elseif ($value instanceof \DateTime) {
            $this->value = (\DateTimeImmutable::createFromMutable($value))->setTime(0, 0, 0, 0);
        } elseif ($value instanceof \DateTimeImmutable) {
            $this->value = $value->setTime(0, 0, 0, 0);
        } else {
            $this->value = $value;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_date();
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : Entry
    {
        return new self($this->name, $this->value ? clone $this->value : null, $this->metadata);
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type, $entry->type) && $this->value() == $entry->value();
    }

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
        $value = $this->value;

        if ($value === null) {
            return '';
        }

        return $value->format('Y-m-d');
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\DateTimeInterface
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
