<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\Types\Exception\CastingException;

use function abs;
use function assert;
use function Flow\Types\DSL\type_datetime;
use function intdiv;
use function is_int;
use function sprintf;

use const PHP_VERSION_ID;

final readonly class DateTimePhysical implements Physical
{
    public function __construct(
        private DateTimeZone $zone,
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DateTimeInterface);

        $us = ($value->getTimestamp() * 1_000_000) + (int) $value->format('u');

        // an i64 overflow of the multiplication or the addition yields a float at runtime
        // @mago-ignore analysis:impossible-condition,redundant-type-comparison
        if (!is_int($us)) {
            throw new CastingException(
                $value,
                type_datetime(),
                reason: 'outside the microsecond range of a datetime column',
            );
        }

        return $us;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_int($physical));

        if (PHP_VERSION_ID >= 80400) {
            $seconds = intdiv($physical, 1_000_000);
            $micros = $physical % 1_000_000;

            if ($micros < 0) {
                $micros += 1_000_000;
                $seconds--;
            }

            // @mago-ignore analysis:unavailable-method
            $instant = DateTimeImmutable::createFromTimestamp($seconds);

            // @mago-ignore analysis:unavailable-method
            return $instant->setMicrosecond($micros)->setTimezone($this->zone);
        }

        return (new DateTimeImmutable(sprintf(
            '@%s%d.%06d',
            $physical < 0 ? '-' : '',
            intdiv(abs($physical), 1_000_000),
            abs($physical) % 1_000_000,
        )))->setTimezone($this->zone);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];
        $modern = PHP_VERSION_ID >= 80400;

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === null) {
                $values[] = null;

                continue;
            }

            assert(is_int($physical));

            if ($modern) {
                $seconds = intdiv($physical, 1_000_000);
                $micros = $physical % 1_000_000;

                if ($micros < 0) {
                    $micros += 1_000_000;
                    $seconds--;
                }

                // @mago-ignore analysis:unavailable-method
                $instant = DateTimeImmutable::createFromTimestamp($seconds);
                // @mago-ignore analysis:unavailable-method
                $values[] = $instant->setMicrosecond($micros)->setTimezone($this->zone);

                continue;
            }

            $values[] = (new DateTimeImmutable(sprintf(
                '@%s%d.%06d',
                $physical < 0 ? '-' : '',
                intdiv(abs($physical), 1_000_000),
                abs($physical) % 1_000_000,
            )))->setTimezone($this->zone);
        }

        return $values;
    }
}
