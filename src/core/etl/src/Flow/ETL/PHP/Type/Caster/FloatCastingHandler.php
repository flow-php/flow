<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;

final class FloatCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ScalarType && $type->isFloat();
    }

    public function value(mixed $value, Type $type) : mixed
    {
        if ($value instanceof \DateTimeImmutable) {
            return (float) $value->format('Uu');
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            return (float) ($endTime->format('Uu')) - (float) ($reference->format('Uu'));
        }

        try {
            return (float) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
