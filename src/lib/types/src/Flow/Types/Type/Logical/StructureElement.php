<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;

/**
 * TOptional mirrors the runtime flag into the type so analyzers can mark the shape key optional
 * without reading the call expression.
 *
 * @template-covariant T
 * @template-covariant TOptional of bool
 */
final readonly class StructureElement
{
    /**
     * @param Type<T> $type
     * @param TOptional $optional
     */
    public function __construct(
        public int|string $name,
        public Type $type,
        public bool $optional = false,
    ) {
        if ('' === $name) {
            throw new InvalidArgumentException('Structure element name cannot be empty.');
        }
    }
}
