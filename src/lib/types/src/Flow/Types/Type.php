<?php

declare(strict_types=1);

namespace Flow\Types;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;

/**
 * @template-covariant T
 */
interface Type
{
    /**
     * Checks that the value is of the type of this object, throwing when it is not.
     * Unlike cast(), this method never performs any conversion — the value must already match the type.
     * Unlike isValid(), this method returns the value (narrowed to T) instead of a boolean, and always narrows the static type.
     *
     * @throws InvalidTypeException
     *
     * @return T
     *
     * @phpstan-assert T $value
     */
    public function assert(mixed $value): mixed;

    /**
     * Takes a value and when necessary casts it to the type of this object.
     * When value is already of the type of this object, it is returned as is.
     * When cast method is used, it will additionally narrow the type of the returned value for static analysis tools.
     *
     * @throws CastingException
     *
     * @return T
     */
    public function cast(mixed $value): mixed;

    /**
     * Checks if the value is of the type of this object, returning a boolean instead of throwing.
     * When this method returns true, static analysis tools narrow the value's type to T at the call site
     * (via @phpstan-assert-if-true). Use this when you want to branch on the result; use assert() when you
     * want the call to fail loudly on a mismatch.
     *
     * @phpstan-assert-if-true T $value
     */
    public function isValid(mixed $value): bool;

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array;

    /**
     * Returns a string representation of the type.
     *
     * - string - for type_string()
     * - int - for type_int()
     * - ?float - for type_optional(type_float())
     */
    public function toString(): string;
}
