<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_object;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\DateTimeInterface, array{name: string, value: \DateTimeInterface, type: ObjectType}>
 */
final class DateTimeEntry implements \Stringable, Entry
{
    use EntryRef;

    private readonly ObjectType $type;

    private readonly \DateTimeInterface $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, \DateTimeInterface|string $value)
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

        $this->type = type_object($this->value::class);
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'value' => $this->value, 'type' => $this->type];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
        $this->type = $data['type'];
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
        return $this->value()->format(\DateTimeInterface::ATOM);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : \DateTimeInterface
    {
        return $this->value;
    }
}
