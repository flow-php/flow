<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\{Caster, Native\FloatType, Type};

final class FloatCastingHandler implements CastingHandler
{
    /**
     * @param Type<float> $type
     */
    public function supports(Type $type) : bool
    {
        return $type instanceof FloatType;
    }

    public function value(mixed $value, Type $type, Caster $caster, Options $options) : float
    {
        /**
         * @var FloatType $type
         */
        if (\is_float($value)) {
            return \round($value, $type->precision, $options->get(Options::FLOAT_ROUNDING_MODE)->value);
        }

        if ($value instanceof \DOMElement) {
            return \round((float) $value->nodeValue, $type->precision, $options->get(Options::FLOAT_ROUNDING_MODE)->value);
        }

        if ($value instanceof \DateTimeImmutable) {
            return \round((float) $value->format('Uu'), $type->precision, $options->get(Options::FLOAT_ROUNDING_MODE)->value);
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            return \round((float) ($endTime->format('Uu')) - (float) ($reference->format('Uu')), $type->precision, $options->get(Options::FLOAT_ROUNDING_MODE)->value);
        }

        try {
            return \round((float) $value, $type->precision, $options->get(Options::FLOAT_ROUNDING_MODE)->value);
        } catch (\Throwable $e) {
            throw new CastingException($value, $type);
        }
    }
}
