<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DateInterval;
use Flow\Types\Exception\CastingException;

use function abs;
use function assert;
use function Flow\Types\DSL\type_time;
use function intdiv;
use function is_int;
use function round;
use function sprintf;

final readonly class TimePhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DateInterval);

        if ($value->y !== 0 || $value->m !== 0) {
            throw new CastingException(
                $value,
                type_time(),
                reason: "Relative DateInterval (with months/years) can't be stored in a time column",
            );
        }

        $seconds = ((($value->d * 24) + $value->h) * 3600) + ($value->i * 60) + $value->s;
        // an i64 overflow yields a float at runtime
        // @mago-ignore analysis:redundant-type-comparison,redundant-condition
        $us = is_int($seconds) ? ($seconds * 1_000_000) + (int) round($value->f * 1_000_000) : null;

        // @mago-ignore analysis:impossible-condition,redundant-type-comparison
        if (!is_int($us)) {
            throw new CastingException($value, type_time(), reason: 'outside the microsecond range of a time column');
        }

        return $value->invert === 1 ? -$us : $us;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_int($physical));

        $magnitude = abs($physical);
        $interval = new DateInterval(sprintf(
            'PT%dH%dM%dS',
            intdiv($magnitude, 3_600_000_000),
            intdiv($magnitude, 60_000_000) % 60,
            intdiv($magnitude, 1_000_000) % 60,
        ));
        // @mago-ignore analysis:invalid-property-write
        $interval->f = ($magnitude % 1_000_000) / 1_000_000;
        // @mago-ignore analysis:invalid-property-write
        $interval->invert = $physical < 0 ? 1 : 0;

        return $interval;
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === null) {
                $values[] = null;

                continue;
            }

            assert(is_int($physical));

            $magnitude = abs($physical);
            $interval = new DateInterval(sprintf(
                'PT%dH%dM%dS',
                intdiv($magnitude, 3_600_000_000),
                intdiv($magnitude, 60_000_000) % 60,
                intdiv($magnitude, 1_000_000) % 60,
            ));
            // @mago-ignore analysis:invalid-property-write
            $interval->f = ($magnitude % 1_000_000) / 1_000_000;
            // @mago-ignore analysis:invalid-property-write
            $interval->invert = $physical < 0 ? 1 : 0;
            $values[] = $interval;
        }

        return $values;
    }
}
