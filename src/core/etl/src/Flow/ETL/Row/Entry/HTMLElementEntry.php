<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function class_exists;
use function Flow\Types\DSL\type_equals;
use function sprintf;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

/**
 * @template-covariant T of HTMLElement|null
 *
 * @implements Entry<T>
 */
final class HTMLElementEntry implements Entry
{
    use EntryRef;

    private HTMLElementDefinition $definition;

    /**
     * @param T $value
     */
    public function __construct(
        private readonly string $name,
        private readonly ?HTMLElement $value,
        ?Metadata $metadata = null,
    ) {
        $this->definition = new HTMLElementDefinition(
            $this->name,
            $this->value === null,
            $metadata ?: Metadata::empty(),
        );
    }

    /**
     * @return self<HTMLElement>
     */
    public static function fromString(string $name, string $value, ?Metadata $metadata = null): self
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            throw new RuntimeException('HTMLElementEntry requires PHP 8.4+ (\Dom\HTMLDocument is not available).');
        }

        $document = HTMLDocument::createFromString($value, LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);
        $documentElement = $document->documentElement;

        if (!$documentElement instanceof HTMLElement) {
            throw new RuntimeException(sprintf('Given string "%s" does not produce a valid HTML element', $value));
        }

        return new self($name, $documentElement, $metadata);
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

    /**
     * @return Type<HTMLElement>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?HTMLElement
    {
        return $this->value;
    }
}
