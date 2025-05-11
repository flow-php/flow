<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\ETL\PHP\Type\Type;

/**
 * @implements Type<float>
 */
final readonly class FloatType implements Type
{
    public function assert(mixed $value) : float
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : float
    {
        /**
         * @var FloatType $type
         */
        if (\is_float($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            return (float) $value->nodeValue;
        }

        if ($value instanceof \DateTimeImmutable) {
            return (float) $value->format('Uu');
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            return (float) $endTime->format('Uu') - (float) $reference->format('Uu');
        }

        try {
            return (float) $value;
        } catch (\Throwable) {
            throw new CastingException($value, $type);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_float($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'float',
        ];
    }

    public function toString() : string
    {
        return 'float';
    }
}
