<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidArgumentException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

/**
 * @template TMin of int
 * @template TMax of int
 *
 * @implements Type<int<TMin, TMax>>
 */
final readonly class IntegerRangeType implements Type
{
    /**
     * @param TMin $min
     * @param TMax $max
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private int $min,
        private int $max,
    ) {
        if ($this->min > $this->max) {
            throw new InvalidArgumentException('Minimum value cannot be greater than maximum value.');
        }
    }

    /**
     * @param array{type:string, min: TMin, max: TMax} $data
     *
     * @return self<TMin, TMax>
     */
    public static function fromArray(array $data) : self
    {
        return new self((int) $data['min'], (int) $data['max']);
    }

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

                return $this->assert((int) ($endTime->format('Uu')) - (int) ($reference->format('Uu')));
            }

            if (\is_object($value)) {
                throw new CastingException($value, $this);
            }

            return $this->assert((int) $value);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_int($value) && $value >= $this->min && $value <= $this->max;
    }

    public function normalize() : array
    {
        return [
            'type' => 'integer_range',
            'min' => $this->min,
            'max' => $this->max,
        ];
    }

    public function toString() : string
    {
        $min = $this->min === PHP_INT_MIN ? 'min' : $this->min;
        $max = $this->max === PHP_INT_MAX ? 'max' : $this->max;

        return 'integer<' . $min . ', ' . $max . '>';
    }
}
