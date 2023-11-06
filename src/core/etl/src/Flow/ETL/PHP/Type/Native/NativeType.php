<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

interface NativeType extends Type
{
    public static function fromString(string $value) : self;
}
