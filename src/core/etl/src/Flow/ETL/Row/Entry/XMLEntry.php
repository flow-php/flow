<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function base64_decode;
use function base64_encode;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function gzcompress;
use function gzuncompress;
use function is_string;
use function sprintf;

/**
 * @implements Entry<?\DOMDocument>
 */
final class XMLEntry implements Entry
{
    use EntryRef;

    private XMLDefinition $definition;

    private readonly ?DOMDocument $value;

    public function __construct(
        private readonly string $name,
        DOMDocument|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if (is_string($value)) {
            $doc = new DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new InvalidArgumentException(sprintf('Given string "%s" is not valid XML', $value));
            }

            $this->value = $doc;
        } else {
            $this->value = $value;
        }

        $this->definition = new XMLDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __serialize(): array
    {
        return [
            'name' => $this->name,
            /** @phpstan-ignore-next-line  */
            'value' => $this->value === null ? null : base64_encode(gzcompress($this->toString())),
        ];
    }

    public function __toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->toString();
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
            $this->definition = new XMLDefinition($this->name, true, Metadata::empty());

            return;
        }

        /** @phpstan-ignore-next-line  */
        $xmlString = gzuncompress(base64_decode((string) $data['value'], true));
        $doc = new DOMDocument();

        /** @phpstan-ignore-next-line  */
        if (!@$doc->loadXML($xmlString)) {
            throw new InvalidArgumentException(sprintf('Given string "%s" is not valid XML', $xmlString));
        }

        $this->value = $doc;
        $this->definition = new XMLDefinition($this->name, false, Metadata::empty());
    }

    public function definition(): XMLDefinition
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

        if ($entry->value?->documentElement === null && $this->value?->documentElement === null) {
            return true;
        }

        return $entry->value()?->C14N() === $this->value?->C14N();
    }

    public function map(callable $mapper): static
    {
        $mappedValue = $mapper($this->value());
        $mappedValue = type_optional(type_instance_of(DOMDocument::class))->assert($mappedValue);

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

        /** @phpstan-ignore-next-line */
        return $this->value->saveXML($this->value->documentElement);
    }

    public function type(): Type
    {
        return $this->definition->type();
    }

    public function value(): ?DOMDocument
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
