<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_html, type_optional};
use Dom\HTMLDocument;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?HTMLDocument>
 */
final class HTMLEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<HTMLDocument>
     */
    private readonly Type $type;

    private ?HTMLDocument $value;

    public function __construct(
        private readonly string $name,
        HTMLDocument|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (\is_string($value)) {
            $this->value = HTMLDocument::createFromString($value, \LIBXML_NOERROR);
        } else {
            $this->value = $value;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_html();
    }

    #[\Override]
    public function __toString() : string
    {
        return $this->toString();
    }

    #[\Override]
    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, null === $this->value, $this->metadata);
    }

    #[\Override]
    public function duplicate() : self
    {
        return new self($this->name, $this->value ? clone $this->value : null, $this->metadata);
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

        return $entry->value()?->saveHtml() === $this->value?->saveHtml();
    }

    #[\Override]
    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value));
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
        if (null === $this->value) {
            return '';
        }

        return $this->value->saveHtml();
    }

    #[\Override]
    public function type() : Type
    {
        return $this->type;
    }

    #[\Override]
    public function value() : ?HTMLDocument
    {
        return $this->value;
    }

    #[\Override]
    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
