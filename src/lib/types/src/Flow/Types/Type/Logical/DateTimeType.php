<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\String\StringTemporalParts;
use Throwable;

use function checkdate;
use function is_bool;
use function is_numeric;
use function is_string;
use function preg_match;
use function substr;

/**
 * @template T of \DateTimeInterface
 *
 * @implements Type<T>
 */
final readonly class DateTimeType implements Type
{
    public const string ISO_DATE_TIME = '/^(\d{4})-(\d{2})-(\d{2})[T ](?:[01]\d|2[0-4]):[0-5]\d(?::(?:[0-5]\d|60)(?:\.\d{1,9})?)?(Z|[+-](?:(?:[01]\d|2[0-4]):?[0-5]\d|\d{2}))?$/';

    public function assert(mixed $value): DateTimeInterface
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DateTimeInterface
    {
        if ($value instanceof DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof DateTime) {
            return DateTimeImmutable::createFromMutable($value);
        }

        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (is_string($value)) {
                $date = [];

                if (
                    preg_match(self::ISO_DATE_TIME, $value, $date) === 1
                    && checkdate((int) $date[2], (int) $date[3], (int) $date[1])
                ) {
                    if (($date[4] ?? '') === 'Z') {
                        // timelib resolves the "Z" abbreviation by scanning its whole abbreviation table, ten
                        // times the cost of parsing the rest; the zone handed in builds the identical object
                        static $zulu = new DateTimeZone('Z');

                        return new DateTimeImmutable(substr($date[0], 0, -1), $zulu);
                    }

                    return new DateTimeImmutable($value);
                }

                if (StringTemporalParts::isoDate($value)) {
                    return new DateTimeImmutable($value);
                }

                $parts = StringTemporalParts::from($value);

                if (!$parts->isDate() && !$parts->isDateTime()) {
                    // DateTimeImmutable resolves '', 'now' and '+12' against the wall clock, so the
                    // same input written twice would produce two different values
                    throw new CastingException($value, $this, reason: 'value is not a calendar date');
                }

                return new DateTimeImmutable($value);
            }

            if (is_numeric($value)) {
                return new DateTimeImmutable('@' . $value);
            }

            if (is_bool($value)) {
                return new DateTimeImmutable('@' . (int) $value);
            }

            if ($value instanceof DateInterval) {
                return (new DateTimeImmutable('@0'))->add($value);
            }
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DateTimeInterface;
    }

    public function normalize(): array
    {
        return [
            'type' => 'datetime',
        ];
    }

    public function toString(): string
    {
        return 'datetime';
    }
}
