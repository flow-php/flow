<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

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

        try {
            return (int) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable) {
            throw new CastingException($value, $type);
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
