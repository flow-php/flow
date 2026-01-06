<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use function Flow\Types\DSL\{type_enum, type_equals};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Row\{Entry, EntryReference, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type\Native\EnumType;

/**
 * @template TEnum of \UnitEnum
 *
 * @implements Definition<TEnum>
 */
final class EnumDefinition implements Definition
{
    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @var EnumType<TEnum>
     */
    private readonly EnumType $type;

    /**
     * @param class-string<TEnum> $enumClass
     */
    public function __construct(
        string|Reference $ref,
        private readonly string $enumClass,
        private readonly bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        if ($enumClass !== \UnitEnum::class && !\enum_exists($enumClass)) {
            throw new InvalidArgumentException(\sprintf('Enum of type "%s" not found', $enumClass));
        }

        $this->ref = EntryReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
        /** @var EnumType<TEnum> $type */
        $type = type_enum($enumClass);
        $this->type = $type;
    }

    /**
     * @param array<array-key, mixed> $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value) : static
    {
        $this->metadata = $this->metadata->add($key, $value);

        return $this;
    }

    public function entry() : Reference
    {
        return $this->ref;
    }

    /**
     * @return class-string<TEnum>
     */
    public function enumClass() : string
    {
        return $this->enumClass;
    }

    public function isCompatible(Definition $definition) : bool
    {
        if (!$this->ref->is($definition->entry())) {
            return false;
        }

        if (!$this->nullable && $definition->isNullable()) {
            return false;
        }

        return type_equals($this->type, $definition->type());
    }

    public function isNullable() : bool
    {
        return $this->nullable;
    }

    public function isSame(Definition $definition) : bool
    {
        if ($this->nullable !== $definition->isNullable()) {
            return false;
        }

        if (!type_equals($this->type, $definition->type())) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata());
    }

    public function makeNullable(bool $nullable = true) : static
    {
        return new self($this->ref, $this->enumClass, $nullable, $this->metadata);
    }

    public function matches(Entry $entry) : bool
    {
        if ($this->isNullable() && $entry->is($this->ref)) {
            return true;
        }

        if (!$entry->is($this->ref)) {
            return false;
        }

        return $entry->type() instanceof EnumType;
    }

    public function merge(Definition $definition) : Definition
    {
        if (!$this->ref->is($definition->entry())) {
            throw new RuntimeException(\sprintf(
                'Cannot merge different definitions, %s and %s',
                $this->ref->name(),
                $definition->entry()->name()
            ));
        }

        $thisFromNull = $this->metadata->has(Metadata::FROM_NULL);
        $defFromNull = $definition->metadata()->has(Metadata::FROM_NULL);

        if ($thisFromNull && $defFromNull) {
            return $this->makeNullable()->setMetadata(
                $this->metadata->merge($definition->metadata())
            );
        }

        if ($thisFromNull) {
            return $definition->makeNullable()->setMetadata(
                $definition->metadata()->remove(Metadata::FROM_NULL)->merge($this->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if ($defFromNull) {
            return $this->makeNullable()->setMetadata(
                $this->metadata->remove(Metadata::FROM_NULL)->merge($definition->metadata()->remove(Metadata::FROM_NULL))
            );
        }

        if ($definition instanceof self && $definition->enumClass === $this->enumClass) {
            return new self(
                $this->ref,
                $this->enumClass,
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata)
            );
        }

        if ($definition instanceof StringDefinition) {
            return new StringDefinition(
                $this->ref,
                $this->nullable || $definition->isNullable(),
                $this->metadata->merge($definition->metadata())
            );
        }

        throw new RuntimeException(\sprintf(
            'Cannot merge %s with %s',
            self::class,
            $definition::class
        ));
    }

    public function metadata() : Metadata
    {
        return $this->metadata;
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize() : array
    {
        return [
            'ref' => $this->ref->name(),
            'type' => $this->type->normalize(),
            'nullable' => $this->nullable,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    public function nullable() : static
    {
        return $this->makeNullable();
    }

    public function rename(string $newName) : static
    {
        return new self($newName, $this->enumClass, $this->nullable, $this->metadata);
    }

    public function setMetadata(Metadata $metadata) : static
    {
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @return EnumType<TEnum>
     */
    public function type() : EnumType
    {
        return $this->type;
    }
}
