<?php declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\DOMNode, array{name: string, value: \DOMNode}>
 */
final class XMLNodeEntry implements \Stringable, Entry
{
    use EntryRef;

    public function __construct(private readonly string $name, private readonly \DOMNode $value)
    {
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
        ];
    }

    public function __toString() : string
    {
        /**
         * @psalm-suppress PossiblyNullReference
         *
         * @phpstan-ignore-next-line
         */
        return $this->value->ownerDocument->saveXML($this->value);
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
    }

    public function definition() : Definition
    {
        return Definition::xml_node($this->ref(), false);
    }

    public function is(Reference|string $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        if (!$entry instanceof self || !$this->is($entry->name())) {
            return false;
        }

        return $this->value->C14N() === $entry->value->C14N();
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value()));
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
        /**
         * @psalm-suppress PossiblyNullReference
         *
         * @phpstan-ignore-next-line
         */
        return $this->value->ownerDocument->saveXML($this->value);
    }

    public function value() : \DOMNode
    {
        return $this->value;
    }
}
