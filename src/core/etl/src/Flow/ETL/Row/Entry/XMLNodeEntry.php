<?php declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_object;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\DOMNode>
 */
final class XMLNodeEntry implements \Stringable, Entry
{
    use EntryRef;

    private readonly ObjectType $type;

    public function __construct(private readonly string $name, private readonly \DOMNode $value)
    {
        $this->type = type_object($this->value::class);
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
            'type' => $this->type,
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
        $this->type = $data['type'];
    }

    public function definition() : Definition
    {
        return Definition::xml_node($this->ref(), $this->type->nullable());
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

        if (!$this->type->isEqual($entry->type)) {
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

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : \DOMNode
    {
        return $this->value;
    }
}
