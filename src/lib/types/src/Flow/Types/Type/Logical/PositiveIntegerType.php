<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Type;

/**
 * @implements Type<int<0, max>>
 */
final readonly class PositiveIntegerType implements Type
{
    public function assert(mixed $value) : int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {

            if ($value instanceof \DOMElement) {
                return $this->assert((int) $value->nodeValue);
            }

            if ($value instanceof \DateTimeImmutable) {
                return $this->assert((int) $value->format('Uu'));
            }

            if ($value instanceof \DateInterval) {
                $reference = new \DateTimeImmutable();
                $endTime = $reference->add($value);

                return $this->assert(((int) $endTime->format('Uu')) - (int) ($reference->format('Uu')));
            }

            return $this->assert((int) $value);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_int($value) && $value > 0;
    }

    public function normalize() : array
    {
        return [
            'type' => 'positive_integer',
        ];
    }

    public function toString() : string
    {
        return 'positive_integer';
    }
}
