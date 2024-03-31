<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_datetime;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?\DateTimeInterface>
 */
final class DateTimeEntry implements Entry
{
    use EntryRef;

    private readonly DateTimeType $type;

    private readonly ?\DateTimeInterface $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, \DateTimeInterface|string|null $value)
    {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\is_string($value)) {
            try {
                $this->value = new \DateTimeImmutable($value);
            } catch (\Exception $e) {
                throw new InvalidArgumentException("Invalid value given: '{$value}', reason: " . $e->getMessage(), previous: $e);
            }
        } elseif ($value instanceof \DateTime) {
            $this->value = \DateTimeImmutable::createFromMutable($value);
        } else {
            $this->value = $value;
        }

        $this->type = type_datetime($this->value === null);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::dateTime($this->name, $this->type->nullable());
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
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && $this->value() == $entry->value();
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

        return $value->format(\DateTimeInterface::ATOM);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\DateTimeInterface
    {
        return $this->value;
    }
}
