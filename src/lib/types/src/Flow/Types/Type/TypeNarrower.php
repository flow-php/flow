<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;

interface TypeNarrower
{
    public static function narrow(Type $type, mixed $value) : Type;
}
