<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\PHP\Type\Type;

final class CastingException extends RuntimeException
{
    public function __construct(public readonly mixed $value, public readonly Type $type)
    {
        parent::__construct(\sprintf("Can't cast \"%s\" into \"%s\" type", \gettype($value), $type->toString()));
    }
}
