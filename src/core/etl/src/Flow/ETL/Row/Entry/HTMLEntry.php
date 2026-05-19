<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Dom\HTMLDocument;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function class_exists;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_optional;
use function is_string;

use const LIBXML_NOERROR;

/**
 * @implements Entry<?HTMLDocument>
 */
final class HTMLEntry implements Entry
{
    use EntryRef;

    private HTMLDefinition $definition;

    private ?HTMLDocument $value;

    public function __construct(
        private readonly string $name,
        HTMLDocument|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (class_exists('\Dom\HTMLDocument') && is_string($value)) {
            $this->value = HTMLDocument::createFromString($value, LIBXML_NOERROR);
        } elseif (is_string($value)) {
            throw new RuntimeException('HTMLEntry requires PHP 8.4+ (\Dom\HTMLDocument is not available).');
        } else {
            $this->value = $value;
        }

        $this->definition = new HTMLDefinition($this->name, null === $this->value, $metadata ?: Metadata::empty());
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): HTMLDefinition
    {
        return $this->definition;
    }

    public function duplicate(): static
    {
        return new self($this->name, $this->value ? clone $this->value : null, $this->definition->metadata());
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

        return $entry->value()?->saveHtml() === $this->value?->saveHtml();
    }

    public function map(callable $mapper): static
    {
        return new self($this->name, type_optional(type_html())->assert($mapper($this->value)));
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
        if (null === $this->value) {
            return '';
        }

        return $this->value->saveHtml();
    }

    public function type(): Type
    {
        return $this->definition->type();
    }

    public function value(): ?HTMLDocument
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional(type_html())->assert($value), $this->definition->metadata());
    }
}
