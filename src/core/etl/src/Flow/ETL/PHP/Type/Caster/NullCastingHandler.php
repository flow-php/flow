<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Type;

final class NullCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof NullType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        return null;
    }
}
