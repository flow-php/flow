<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use function Flow\ETL\DSL\type_date;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Logical\DateType, Type};

final class DateCastingHandler implements CastingHandler
{
    /**
     * @param Type<\DateTimeImmutable> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof DateType;
    }

    /**
     * @param Type<\DateTimeImmutable> $type
     */
    public function value(mixed $value, Type $type, Caster $caster, Options $options) : \DateTimeImmutable
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value->setTime(0, 0, 0, 0);
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        if ($value instanceof \DateTime) {
            return \DateTimeImmutable::createFromMutable($value)->setTime(0, 0, 0, 0);
        }

        try {
            if (\is_string($value)) {
                return (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0);
            }

            if (\is_numeric($value)) {
                return (new \DateTimeImmutable('@' . $value))->setTime(0, 0, 0, 0);
            }

            if (\is_bool($value)) {
                /* @phpstan-ignore-next-line */
                return (new \DateTimeImmutable('@' . $value))->setTime(0, 0, 0, 0);
            }

            if ($value instanceof \DateInterval) {
                return (new \DateTimeImmutable('@0'))->add($value)->setTime(0, 0, 0, 0);

            }
        } catch (\Throwable) {
            throw new CastingException($value, type_date());
        }

        throw new CastingException($value, $type);
    }
}
