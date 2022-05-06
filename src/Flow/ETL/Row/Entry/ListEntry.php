<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\Metadata;

/**
 * @template T
 * @implements Entry<array<T>, array{name: string, type: Type, value: array<T>}>
 * @psalm-immutable
 */
final class ListEntry implements Entry, TypedCollection
{
    /**
     * @param string $name
     * @param Type $type
     * @param array<T> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly Type $type,
        private readonly array $value
    ) {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\count($value) && !\array_is_list($value)) {
            throw new InvalidArgumentException('Expected list of ' . $type->toString() . ' got array with not sequential integer indexes');
        }

        if (!$type->isValid($value)) {
            throw new InvalidArgumentException('Expected list of ' . $type->toString() . ' got different types.');
        }
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'type' => $this->type, 'value' => $this->value];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->type = $data['type'];
        $this->value = $data['value'];
    }

    public function definition() : Definition
    {
        /** @psalm-suppress ImpureMethodCall */
        return Definition::list(
            $this->name,
            $this->type,
            metadata: Metadata::empty()
                ->add(Definition::METADATA_LIST_ENTRY_TYPE, $this->type())
        );
    }

    public function is(string $name) : bool
    {
        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) &&
            $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value())
            && $this->type->isEqual($entry->type);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $this->type, $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->type, $this->value);
    }

    public function toString() : string
    {
        return \json_encode($this->value(), JSON_THROW_ON_ERROR);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : array
    {
        return $this->value;
    }
}
