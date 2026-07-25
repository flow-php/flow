<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Dom\XMLDocument;
use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use ReflectionProperty;
use RuntimeException;

use function base64_decode;
use function base64_encode;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;
use function gzcompress;
use function gzuncompress;
use function sprintf;

/**
 * @template-covariant T of \DOMDocument|XMLDocument|null
 *
 * @implements Entry<T>
 */
final class XMLEntry implements Entry
{
    use EntryRef;

    private XMLDefinition $definition;

    /**
     * @param T $value
     */
    public function __construct(
        private readonly string $name,
        private readonly DOMDocument|XMLDocument|null $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new XMLDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __serialize(): array
    {
        if ($this->value === null) {
            return ['name' => $this->name, 'value' => null];
        }

        $compressed = gzcompress($this->toString());

        if ($compressed === false) {
            throw new RuntimeException('Failed to gzcompress XML entry value.');
        }

        return ['name' => $this->name, 'value' => base64_encode($compressed)];
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
        $name = type_string()->assert($data['name']);
        (new ReflectionProperty($this, 'name'))->setValue($this, $name);

        if ($data['value'] === null) {
            (new ReflectionProperty($this, 'value'))->setValue($this, null);
            $this->definition = new XMLDefinition($name, true, Metadata::empty());

            return;
        }

        $encoded = type_string()->assert($data['value']);
        $decoded = base64_decode($encoded, true);

        if ($decoded === false) {
            throw new InvalidArgumentException(sprintf('Given value "%s" is not valid base64', $encoded));
        }

        $xmlString = gzuncompress($decoded);

        if ($xmlString === false) {
            throw new InvalidArgumentException('Given value is not valid gzcompressed XML');
        }

        $doc = new DOMDocument();

        if (!@$doc->loadXML($xmlString)) {
            throw new InvalidArgumentException(sprintf('Given string "%s" is not valid XML', $xmlString));
        }

        (new ReflectionProperty($this, 'value'))->setValue($this, $doc);
        $this->definition = new XMLDefinition($name, false, Metadata::empty());
    }

    public function definition(): XMLDefinition
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

        if ($entry->value?->documentElement === null && $this->value?->documentElement === null) {
            return true;
        }

        return $entry->value()?->C14N() === $this->value?->C14N();
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

        if ($this->value instanceof XMLDocument) {
            $serialized = $this->value->saveXml($this->value->documentElement);
        } else {
            // @mago-ignore analysis:possibly-invalid-argument,possibly-invalid-argument
            $serialized = $this->value->saveXML($this->value->documentElement);
        }

        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }

    /**
     * @return Type<DOMDocument|XMLDocument>
     */
    public function type(): Type
    {
        return $this->definition->type();
    }

    /**
     * @return T
     */
    public function value(): DOMDocument|XMLDocument|null
    {
        return $this->value;
    }
}
