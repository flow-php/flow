<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DateTimeZone;

use function assert;
use function is_string;

final readonly class TimeZonePhysical implements Physical
{
    public function __construct(
        private TimeZones $zones = new TimeZones(),
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DateTimeZone);

        return $value->getName();
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return $this->zones->get($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : $this->zones->get($physical);
        }

        return $values;
    }
}
