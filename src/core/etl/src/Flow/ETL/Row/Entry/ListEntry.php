<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\Schema\FlowMetadata;
use Flow\ETL\Row\Schema\Metadata;

/**
 * @template T
 *
 * @implements Entry<array<T>, array{name: string, list: ListType, value: array<T>}>
 */
final class ListEntry implements Entry, TypedCollection
{
    use EntryRef;

    private readonly ListType $list;

    /**
     * @param array<T> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        ListElement $element,
        private readonly array $value
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->list = new ListType($element);

        if (!$this->list->isValid($value)) {
            throw InvalidArgumentException::because('Expected list of ' . $element->toString() . ' got different types.');
        }
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'list' => $this->list, 'value' => $this->value];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->list = $data['list'];
        $this->value = $data['value'];
    }

    public function definition() : Definition
    {
        return Definition::list(
            $this->name,
            $this->list->element(),
            metadata: Metadata::with(FlowMetadata::METADATA_LIST_ENTRY_TYPE, $this->type())
        );
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
        return $this->is($entry->name())
            && $entry instanceof self
            && (new ArrayComparison())->equals($this->value, $entry->value())
            && $this->list->isEqual($entry->list);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $this->list->element(), $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->list->element(), $this->value);
    }

    public function toString() : string
    {
        return \json_encode($this->value(), JSON_THROW_ON_ERROR);
    }

    public function type() : ListType
    {
        return $this->list;
    }

    public function value() : array
    {
        return $this->value;
    }
}
