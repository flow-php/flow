<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};

/**
 * @template-covariant T
 */
interface Type
{
    /**
     * @throws InvalidTypeException
     *
     * @return T
     *
     * @phpstan-assert T $value
     */
    public function assert(mixed $value) : mixed;

    /**
     * @throws CastingException
     *
     * @return T
     */
    public function cast(mixed $value) : mixed;

    /**
     * @phpstan-assert-if-true T $value
     */
    public function isValid(mixed $value) : bool;

    /**
     * @return array<string, string>
     */
    public function normalize() : array;

    public function toString() : string;
}
