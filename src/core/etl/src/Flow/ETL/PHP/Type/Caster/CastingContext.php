<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Type;

final class CastingContext
{
    public function __construct(
        private readonly CastingHandler $handler,
        private readonly Type $type,
        private readonly Caster $caster
    ) {
    }

    public function value(mixed $value) : mixed
    {
        if ($value === null && $this->type->nullable()) {
            return null;
        }

        if ($value === null && !$this->type->nullable()) {
            throw new CastingException($value, $this->type);
        }

        return $this->handler->value($value, $this->type, $this->caster);
    }
}
