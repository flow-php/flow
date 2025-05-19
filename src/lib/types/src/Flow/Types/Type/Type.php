<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;

/**
 * @template-covariant T
 */
interface Type
{
    /**
     * Checks if the value is of the type of given type.
     * The difference between this method and cast() is that this method does not perform any casting.
     * The difference between this method and isValid() is that this method also returns passed value narrowing it's type.
     * When assert method is used, it will additionally narrow the type of the returned value for static analysis tools.
     *
     * @throws InvalidTypeException
     *
     * @return T
     *
     * @phpstan-assert T $value
     */
    public function assert(mixed $value) : mixed;

    /**
     * Takes a value and when necessary casts it to the type of this object.
     * When value is already of the type of this object, it is returned as is.
     * When cast method is used, it will additionally narrow the type of the returned value for static analysis tools.
     *
     * @throws CastingException
     *
     * @return T
     */
    public function cast(mixed $value) : mixed;

    /**
     * Checks if the value is of the type of this object.
     * When isValid method is used, it will not narrow the type of the value for static analysis tools.
     *
     * @phpstan-assert-if-true T $value
     */
    public function isValid(mixed $value) : bool;

    /**
     * @return array<string, string>
     */
    public function normalize() : array;

    /**
     * Returns a string representation of the type.
     *
     * - string - for type_string()
     * - int - for type_int()
     * - ?float - for type_optional(type_float())
     */
    public function toString() : string;
}
