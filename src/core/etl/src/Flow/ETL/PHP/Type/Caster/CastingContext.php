<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Type};

/**
 * @template T
 */
final readonly class CastingContext
{
    /**
     * @param CastingHandler $handler
     * @param Type<T> $type
     * @param Caster $caster
     * @param Options $options
     */
    public function __construct(
        private CastingHandler $handler,
        private Type $type,
        private Caster $caster,
        private Options $options,
    ) {
    }

    /**
     * @return ?T
     */
    public function value(mixed $value)
    {
        if ($value === null && $this->type->nullable()) {
            return null;
        }

        if ($value === null && !$this->type->nullable()) {
            throw new CastingException($value, $this->type);
        }

        return $this->handler->value($value, $this->type, $this->caster, $this->options);
    }
}
