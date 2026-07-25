<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Dom\HTMLDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function class_exists;
use function Flow\Types\DSL\type_equals;
use function sprintf;

use const LIBXML_NOERROR;

/**
 * @template-covariant T of HTMLDocument|null
 *
 * @implements Entry<T>
 */
final class HTMLEntry implements Entry
{
    use EntryRef;

    private HTMLDefinition $definition;

    /**
     * @param T $value
     */
    public function __construct(
        private readonly string $name,
        private readonly ?HTMLDocument $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new HTMLDefinition($this->name, null === $this->value, $metadata ?: Metadata::empty());
    }

    /**
     * @return self<HTMLDocument>
     */
    public static function fromString(string $name, string $value, ?Metadata $metadata = null): self
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            throw new RuntimeException('HTMLEntry requires PHP 8.4+ (\Dom\HTMLDocument is not available).');
        }

        // @mago-expect analysis:unavailable-method
        $document = HTMLDocument::createFromString($value, LIBXML_NOERROR);

        // @mago-ignore analysis:impossible-condition
        if (!$document instanceof HTMLDocument) {
            throw new RuntimeException(sprintf('Given string "%s" could not be parsed as HTML', $value));
        }

        return new self($name, $document, $metadata);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function definition(): HTMLDefinition
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

        // @mago-expect analysis:unavailable-method
        // @mago-expect analysis:unavailable-method
        return $entry->value()?->saveHtml() === $this->value?->saveHtml();
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return self<T>
     */
    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        if (null === $this->value) {
            return '';
        }

        // @mago-expect analysis:unavailable-method
        return $this->value->saveHtml();
    }

    /**
     * @return Type<HTMLDocument>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): ?HTMLDocument
    {
        return $this->value;
    }
}
