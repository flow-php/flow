<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_instance_of, type_optional, type_string, type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Type\Logical\{XMLElementType};

/**
 * @implements Entry<?\DOMElement>
 */
final class XMLElementEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<\DOMElement>
     */
    private readonly Type $type;

    private readonly ?\DOMElement $value;

    public function __construct(
        private readonly string $name,
        \DOMElement|string|null $value,
        ?Metadata $metadata = null,
    ) {
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

        $this->metadata = $metadata ?: Metadata::empty();
        $this->value = $value;
        $this->type = type_xml_element();
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
        return (string) $this->value->ownerDocument->saveXML($this->value);
    }

    /**
     * @param array<array-key, mixed> $data
     */
    public function __unserialize(array $data) : void
    {
        type_string()->assert($data['name']);
        type_instance_of(XMLElementType::class)->assert($data['type']);

        $this->name = $data['name'];
        $this->type = $data['type'];

        if ($data['value'] === null) {
            $this->value = null;

            return;
        }

        $element = \gzuncompress(\base64_decode(\is_scalar($data['value']) ? (string) $data['value'] : '', true) ?: '') ?: '';

        $domDocument = new \DOMDocument();
        @$domDocument->loadXML($element);

        /**
         * @phpstan-ignore-next-line
         */
        $this->value = (new \DOMDocument())->importNode($domDocument->documentElement, true);
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : self
    {
        return new self($this->name, type_optional(type_instance_of(\DOMElement::class))->assert($this->value ? $this->value->cloneNode(true) : null), $this->metadata);
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

        if (!type_equals($this->type, $entry->type)) {
            return false;
        }

        return $this->value?->C14N() === $entry->value?->C14N();
    }

    public function map(callable $mapper) : self
    {
        $mappedValue = $mapper($this->value());
        $mappedValue = type_optional(type_instance_of(\DOMElement::class))->assert($mappedValue);

        return new self($this->name, $mappedValue);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : self
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

    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
