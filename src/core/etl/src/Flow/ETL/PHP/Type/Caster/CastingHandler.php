<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\PHP\Type\{Caster, Type};

interface CastingHandler
{
    /**
     * @param Type<mixed> $type
     */
    public function supports(Type $type) : bool;

    /**
     * @template T
     *
     * @param Type<T> $type
     *
     * @return T
     */
    public function value(mixed $value, Type $type, Caster $caster, Options $options) : mixed;
}
