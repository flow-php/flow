<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_html_element, type_instance_of, type_optional};
use Dom\{HTMLDocument, HTMLElement};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?HTMLElement>
 */
final class HTMLElementEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<HTMLElement>
     */
    private readonly Type $type;

    private readonly ?HTMLElement $value;

    public function __construct(
        private readonly string $name,
        HTMLElement|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (\is_string($value)) {
            $document = HTMLDocument::createFromString($value, \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

            $value = $document->documentElement;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->value = $value;
        $this->type = type_html_element();
    }

    #[\Override]
    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->toString();
    }

    #[\Override]
    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    #[\Override]
    public function duplicate() : self
    {
        return new self($this->name, type_optional(type_instance_of(HTMLElement::class))->assert($this->value ? $this->value->cloneNode(true) : null), $this->metadata);
    }

    #[\Override]
    public function is(Reference|string $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    #[\Override]
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

    #[\Override]
    public function map(callable $mapper) : self
    {
        $mappedValue = $mapper($this->value());
        $mappedValue = type_optional(type_instance_of(HTMLElement::class))->assert($mappedValue);

        return new self($this->name, $mappedValue);
    }

    #[\Override]
    public function name() : string
    {
        return $this->name;
    }

    #[\Override]
    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    #[\Override]
    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->innerHTML;
    }

    #[\Override]
    public function type() : Type
    {
        return $this->type;
    }

    #[\Override]
    public function value() : ?HTMLElement
    {
        return $this->value;
    }

    #[\Override]
    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
