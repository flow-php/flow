<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function is_string;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

/**
 * @implements Entry<?HTMLElement>
 */
final class HTMLElementEntry implements Entry
{
    use EntryRef;

    private HTMLElementDefinition $definition;

    private readonly ?HTMLElement $value;

    public function __construct(
        private readonly string $name,
        HTMLElement|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (is_string($value)) {
            $document = HTMLDocument::createFromString($value, LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

            $value = $document->documentElement;
        }

        $this->value = $value;
        $this->definition = new HTMLElementDefinition(
            $this->name,
            $this->value === null,
            $metadata ?: Metadata::empty(),
        );
    }

    public function __toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->toString();
    }

    public function definition(): HTMLElementDefinition
    {
        return $this->definition;
    }

    public function duplicate(): static
    {
        return new self(
            $this->name,
            type_optional(type_instance_of(HTMLElement::class))
                ->assert($this->value ? $this->value->cloneNode(true) : null),
            $this->definition->metadata(),
        );
    }

    public function is(Reference|string $name): bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry): bool
    {
        if (!$entry instanceof self || !$this->is($entry->name())) {
            return false;
        }

        if (!type_equals($this->type(), $entry->type())) {
            return false;
        }

        return $this->value?->C14N() === $entry->value?->C14N();
    }

    public function map(callable $mapper): static
    {
        $mappedValue = $mapper($this->value());
        $mappedValue = type_optional(type_instance_of(HTMLElement::class))->assert($mappedValue);

        return new self($this->name, $mappedValue);
    }

    public function name(): string
    {
        return $this->name;
    }

    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->innerHTML;
    }

    public function type(): Type
    {
        return $this->definition->type();
    }

    public function value(): ?HTMLElement
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
