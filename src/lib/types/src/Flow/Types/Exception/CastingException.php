<?php

declare(strict_types=1);

namespace Flow\Types\Exception;

use Flow\Types\Type;

final class CastingException extends RuntimeException
{
    /**
     * @param mixed $value
     * @param Type<mixed> $type
     * @param null|\Throwable $previous
     */
    public function __construct(public readonly mixed $value, public readonly Type $type, ?\Throwable $previous = null)
    {
        parent::__construct(
            \sprintf("Can't cast \"%s\" into \"%s\" type", \get_debug_type($value), $type->toString()),
            0,
            $previous
        );
    }
}
