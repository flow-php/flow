<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_null;
use function sprintf;

/**
 * @implements Definition<null>
 */
final class NullDefinition implements Definition
{
    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @var Type<null>
     */
    private readonly Type $type;

    public function __construct(string|Reference $ref, ?Metadata $metadata = null)
    {
        $this->ref = EntryReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = type_null();
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        $this->metadata = $this->metadata->add($key, $value);

        return $this;
    }

    public function entry(): Reference
    {
        return $this->ref;
    }

    public function isCompatible(Definition $definition): bool
    {
        if (!$this->ref->is($definition->entry())) {
            return false;
        }

        return type_equals($this->type, $definition->type());
    }

    public function isNullable(): bool
    {
        return true;
    }

    public function isSame(Definition $definition): bool
    {
        if (!$definition->isNullable()) {
            return false;
        }

        if (!type_equals($this->type, $definition->type())) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata());
    }

    public function makeNullable(bool $nullable = true): static
    {
        return new self($this->ref, $this->metadata);
    }

    public function matches(Entry $entry): bool
    {
        return $entry->is($this->ref);
    }

    public function merge(Definition $definition): Definition
    {
        if (!$this->ref->is($definition->entry())) {
            throw new RuntimeException(sprintf(
                'Cannot merge different definitions, %s and %s',
                $this->ref->name(),
                $definition->entry()->name(),
            ));
        }

        if ($definition instanceof self) {
            return new self($this->ref, $definition->metadata()->merge($this->metadata));
        }

        return $definition->makeNullable()->setMetadata($definition->metadata()->merge($this->metadata));
    }

    public function metadata(): Metadata
    {
        return $this->metadata;
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'ref' => $this->ref->name(),
            'type' => $this->type->normalize(),
            'nullable' => true,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    public function rename(string $newName): static
    {
        return new self($newName, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        $this->metadata = $metadata;

        return $this;
    }

    public function type(): Type
    {
        return $this->type;
    }

    /**
     * @return class-string<Entry\NullEntry>
     */
    public function entryClass(): string
    {
        return Entry\NullEntry::class;
    }
}
