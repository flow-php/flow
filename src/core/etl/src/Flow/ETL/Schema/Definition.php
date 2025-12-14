<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

use Flow\ETL\Row\{Entry, Reference};
use Flow\Types\Type;

/**
 * @template-covariant T
 */
interface Definition
{
    /**
     * @param array<array-key, mixed> $value
     *
     * @return static
     */
    public function addMetadata(string $key, int|string|bool|float|array $value) : static;

    public function entry() : Reference;

    /**
     * Checks if another type is compatible with this type. Nullability is validated from a schema evolution perspective.
     * This means that when current type is nullable and the other type is not nullable, it is still compatible.
     * When given type is not nullable and current type is nullable, it is not compatible.
     *
     * @param Definition<mixed> $definition
     */
    public function isCompatible(self $definition) : bool;

    public function isNullable() : bool;

    /**
     * @param Definition<mixed> $definition
     */
    public function isSame(self $definition) : bool;

    /**
     * @return static
     */
    public function makeNullable(bool $nullable = true) : static;

    /**
     * @param Entry<mixed> $entry
     */
    public function matches(Entry $entry) : bool;

    /**
     * @param Definition<mixed> $definition
     *
     * @return Definition<mixed>
     */
    public function merge(self $definition) : self;

    public function metadata() : Metadata;

    /**
     * @return array<string, mixed>
     */
    public function normalize() : array;

    /**
     * @deprecated Use makeNullable() instead
     *
     * @return static
     */
    public function nullable() : static;

    /**
     * @return static
     */
    public function rename(string $newName) : static;

    /**
     * @return static
     */
    public function setMetadata(Metadata $metadata) : static;

    /**
     * @return Type<T>
     */
    public function type() : Type;
}
