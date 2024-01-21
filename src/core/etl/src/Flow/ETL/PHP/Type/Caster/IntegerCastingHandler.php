<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;

final class IntegerCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof ScalarType && $type->isInteger();
    }

    public function value(mixed $value, Type $type, Caster $caster) : mixed
    {
        if (\is_int($value)) {
            return $value;
        }

        if ($value instanceof \DateTimeImmutable) {
            return (int) $value->format('Uu');
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            return (int) ($endTime->format('Uu')) - (int) ($reference->format('Uu'));
        }

        try {
            return (int) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
