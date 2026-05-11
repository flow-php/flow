<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

/**
 * @implements Entry<?\DOMElement>
 */
final class XMLElementEntry implements Entry
{
    use EntryRef;

    private XMLElementDefinition $definition;

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
        }

        $this->value = $value;
        $this->definition = new XMLElementDefinition(
            $this->name,
            $this->value === null,
            $metadata ?: Metadata::empty(),
        );
    }

    public function __serialize(): array
    {
        return [
            'name' => $this->name,
            'value' => $this->value === null ? null : \base64_encode(\gzcompress($this->toString()) ?: ''),
        ];
    }

    public function __toString(): string
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
    public function __unserialize(array $data): void
    {
        type_string()->assert($data['name']);

        $this->name = $data['name'];

        if ($data['value'] === null) {
            $this->value = null;
            $this->definition = new XMLElementDefinition($this->name, true, Metadata::empty());

            return;
        }

        $element = \gzuncompress(\base64_decode(\is_scalar($data['value']) ? (string) $data['value'] : '', true) ?: '')
        ?: '';

        $domDocument = new \DOMDocument();
        @$domDocument->loadXML($element);

        /**
         * @phpstan-ignore-next-line
         */
        $this->value = (new \DOMDocument())->importNode($domDocument->documentElement, true);
        $this->definition = new XMLElementDefinition($this->name, false, Metadata::empty());
    }

    public function definition(): XMLElementDefinition
    {
        return $this->definition;
    }

    public function duplicate(): static
    {
        return new self(
            $this->name,
            type_optional(type_instance_of(\DOMElement::class))
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
        $mappedValue = type_optional(type_instance_of(\DOMElement::class))->assert($mappedValue);

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

        /* @phpstan-ignore-next-line */
        return $this->value->ownerDocument->saveXML($this->value);
    }

    public function type(): Type
    {
        return $this->definition->type();
    }

    public function value(): ?\DOMElement
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
