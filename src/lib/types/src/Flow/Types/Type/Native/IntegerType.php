<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

/**
 * @implements Type<int>
 */
final readonly class IntegerType implements Type
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
                return (int) $value->nodeValue;
            }

            if ($value instanceof \DateTimeImmutable) {
                return (int) $value->format('Uu');
            }

            if ($value instanceof \DateInterval) {
                $reference = new \DateTimeImmutable();
                $endTime = $reference->add($value);

                return (int) ($endTime->format('Uu')) - (int) ($reference->format('Uu'));
            }

            if (\is_object($value)) {
                throw new CastingException($value, $this);
            }

            return (int) $value;
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_int($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'integer',
        ];
    }

    public function toString() : string
    {
        return 'integer';
    }
}
