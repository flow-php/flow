<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_instance_of, type_optional, type_string, type_xml};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Type\Logical\XMLType;

/**
 * @implements Entry<?\DOMDocument>
 */
final class XMLEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<\DOMDocument>
     */
    private readonly Type $type;

    private readonly ?\DOMDocument $value;

    public function __construct(
        private readonly string $name,
        \DOMDocument|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (\is_string($value)) {
            $doc = new \DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new InvalidArgumentException(\sprintf('Given string "%s" is not valid XML', $value));
            }

            $this->value = $doc;
        } else {
            $this->value = $value;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_xml();
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            /** @phpstan-ignore-next-line  */
            'value' => $this->value === null ? null : \base64_encode(\gzcompress($this->toString())),
            'type' => $this->type,
        ];
    }

    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->toString();
    }

    /**
     * @param array<array-key, mixed> $data
     */
    public function __unserialize(array $data) : void
    {
        type_string()->assert($data['name']);
        type_instance_of(XMLType::class)->assert($data['type']);

        $this->name = $data['name'];
        $this->type = $data['type'];

        if ($data['value'] === null) {
            $this->value = null;

            return;
        }

        /** @phpstan-ignore-next-line  */
        $xmlString = \gzuncompress(\base64_decode((string) $data['value'], true));
        $doc = new \DOMDocument();

        /** @phpstan-ignore-next-line  */
        if (!@$doc->loadXML($xmlString)) {
            throw new InvalidArgumentException(\sprintf('Given string "%s" is not valid XML', $xmlString));
        }

        $this->value = $doc;
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : self
    {
        return new self($this->name, $this->value ? clone $this->value : null, $this->metadata);
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

        if ($entry->value?->documentElement === null && $this->value?->documentElement === null) {
            return true;
        }

        return $entry->value()?->C14N() === $this->value?->C14N();
    }

    public function map(callable $mapper) : self
    {
        $mappedValue = $mapper($this->value());
        $mappedValue = type_optional(type_instance_of(\DOMDocument::class))->assert($mappedValue);

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

        /** @phpstan-ignore-next-line */
        return $this->value->saveXML($this->value->documentElement);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\DOMDocument
    {
        return $this->value;
    }

    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
