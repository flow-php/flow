<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_datetime;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\{Caster, Type};

final class DateTimeCastingHandler implements CastingHandler
{
    /**
     * @param Type<\DateTimeImmutable> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof DateTimeType;
    }

    /**
     * @param Type<\DateTimeImmutable> $type
     */
    public function value(mixed $value, Type $type, Caster $caster, Options $options) : \DateTimeImmutable
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
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
        } catch (\Throwable) {
            throw new CastingException($value, type_datetime());
        }

        throw new CastingException($value, $type);
    }
}
