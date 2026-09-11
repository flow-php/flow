<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateInterval;
use DateTimeImmutable;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;

use function intdiv;
use function is_bool;
use function is_finite;
use function is_int;
use function is_numeric;
use function is_string;
use function ltrim;
use function preg_match;
use function str_starts_with;
use function substr;
use function trim;

/**
 * @template T of int<0, max>
 *
 * @implements Type<T>
 */
final readonly class PositiveIntegerType implements Type
{
    /**
     * @return int<0, max>
     */
    public function assert(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return int<0, max>
     */
    public function cast(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        // XML text is a string carrier: coerced here rather than in its own arm below, so it reaches
        // the same range check as any other text instead of saturating through a (int) of its own.
        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        // Canonical integer text ('42', '-42', '0') round-trips by construction, so it needs none of
        // the range work below - and it is what a CSV or JSON read hands over on the hot path.
        $canonicalInteger = is_string($value) && (string) (int) $value === $value;

        // hoisted above the try: the catch below re-wraps without the reason
        if (
            !$canonicalInteger
            && is_string($value)
            && is_numeric($value)
            && preg_match('/^\s*[+-]?[0-9]+\s*$/', $value) === 1
        ) {
            // (int) saturates integer-shaped text silently, so round-trip it exactly
            $trimmed = ltrim(trim($value), '+');
            $negative = str_starts_with($trimmed, '-');
            $digits = ltrim($negative ? substr($trimmed, 1) : $trimmed, '0');

            if ($digits === '') {
                $digits = '0';
            }

            if ((string) (int) $value !== ($negative && $digits !== '0' ? '-' : '') . $digits) {
                throw new CastingException($value, $this, reason: 'value is out of range for integer');
            }
        } elseif (!$canonicalInteger && is_numeric($value)) {
            // a float, or float-shaped text: (int) yields 0 out of range instead of failing
            $asFloat = (float) $value;

            if (!is_finite($asFloat) || $asFloat >= 9223372036854775808.0 || $asFloat < -9223372036854775808.0) {
                throw new CastingException($value, $this, reason: 'value is out of range for integer');
            }
        }

        try {
            if ($value instanceof DateTimeImmutable) {
                return $this->assert((int) $value->format('U'));
            }

            if ($value instanceof DateInterval) {
                $reference = new DateTimeImmutable();
                $endTime = $reference->add($value);

                // the reference is a wall clock, so a sub-second interval straddles a second boundary
                // or not depending on when this runs - subtract in micros, then divide
                return $this->assert(intdiv((int) $endTime->format('Uu') - (int) $reference->format('Uu'), 1_000_000));
            }

            if (is_numeric($value)) {
                return $this->assert((int) $value);
            }

            if (is_bool($value)) {
                return $this->assert($value ? 1 : 0);
            }

            throw new CastingException($value, $this);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        return is_int($value) && $value > 0;
    }

    public function normalize(): array
    {
        return [
            'type' => 'positive_integer',
        ];
    }

    public function toString(): string
    {
        return 'positive_integer';
    }
}
