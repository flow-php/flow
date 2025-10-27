<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_html, type_optional};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Value\HTMLDocument;

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
            $this->value = HTMLDocument::fromString($value);
        } else {
            $this->value = $value;
        }

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_html();
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, null === $this->value, $this->metadata);
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

        return $entry->value()?->toString() === $this->value?->toString();
    }

    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value));
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
        if (null === $this->value) {
            return '';
        }

        return $this->value->toString();
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?HTMLDocument
    {
        return $this->value;
    }

    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
