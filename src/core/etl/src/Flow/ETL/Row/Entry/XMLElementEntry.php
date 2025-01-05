<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_xml_element;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\XMLElementType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?\DOMElement, \DOMElement>
 */
final class XMLElementEntry implements Entry
{
    use EntryRef;

    private readonly XMLElementType $type;

    private readonly ?\DOMElement $value;

    public function __construct(private readonly string $name, \DOMElement|string|null $value)
    {
        if (\is_string($value)) {
            $doc = new \DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new InvalidArgumentException(\sprintf('Given string "%s" is not valid XML', $value));
            }

            $value = $doc->documentElement;
        } elseif ($value instanceof \DOMElement) {
            /** @var \DOMElement $value */
            $value = (new \DOMDocument())->importNode($value, true);
        }

        $this->type = type_xml_element($value === null);
        $this->value = $value;
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value === null ? null : \base64_encode(\gzcompress($this->toString()) ?: ''),
            'type' => $this->type,
        ];
    }

    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        /* @phpstan-ignore-next-line */
        return $this->value->ownerDocument->saveXML($this->value);
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->type = $data['type'];

        if ($data['value'] === null) {
            $this->value = null;

            return;
        }

        $element = \gzuncompress(\base64_decode($data['value'], true) ?: '') ?: '';

        $domDocument = new \DOMDocument();
        @$domDocument->loadXML($element);

        /**
         * @phpstan-ignore-next-line
         */
        $this->value = (new \DOMDocument())->importNode($domDocument->documentElement, true);
    }

    public function definition() : Definition
    {
        return Definition::xml_element($this->ref(), $this->type->nullable());
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

        return $this->value?->C14N() === $entry->value?->C14N();
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
        if ($this->value === null) {
            return '';
        }

        /* @phpstan-ignore-next-line */
        return $this->value->ownerDocument->saveXML($this->value);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\DOMElement
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
