<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\PHP\Type\{Caster, Type};

interface CastingHandler
{
    public function supports(Type $type) : bool;

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : mixed;
}
