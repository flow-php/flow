<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};

/**
 * @template-covariant T of mixed
 */
interface Type
{
    /**
     * @throws InvalidTypeException
     *
     * @return T
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

    public function normalize() : array;

    public function toString() : string;
}
