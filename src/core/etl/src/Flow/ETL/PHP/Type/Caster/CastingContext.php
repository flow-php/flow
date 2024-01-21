<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\PHP\Type\Type;

final class CastingContext
{
    public function __construct(private readonly CastingHandler $handler, private readonly Type $type)
    {
    }

    public function value(mixed $value) : mixed
    {
        return $this->handler->value($value, $this->type);
    }
}
