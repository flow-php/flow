<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_datetime;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Type;

final class DateTimeCastingHandler implements CastingHandler
{
    public function supports(Type $type) : bool
    {
        return $type instanceof DateTimeType;
    }

    public function value(mixed $value, Type $type) : mixed
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof \DateTime) {
            return \DateTimeImmutable::createFromMutable($value);
        }

        try {
            if (\is_string($value)) {
                return new \DateTimeImmutable($value);
            }

            if (\is_numeric($value)) {
                return new \DateTimeImmutable('@' . $value);
            }

            if (\is_bool($value)) {
                /* @phpstan-ignore-next-line */
                return new \DateTimeImmutable('@' . $value);
            }

            if ($value instanceof \DateInterval) {
                return (new \DateTimeImmutable('@0'))->add($value);

            }
        } catch (\Throwable $e) {
            throw new CastingException($value, type_datetime());
        }

        throw new CastingException($value, $type);
    }
}
