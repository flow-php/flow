<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\Types\Exception\CastingException;

use function assert;
use function Flow\Types\DSL\type_date;
use function is_int;

final readonly class DatePhysical implements Physical
{
    private DaysFromCivil $days;

    private DateTimeZone $utc;

    public function __construct()
    {
        $this->days = new DaysFromCivil();
        $this->utc = new DateTimeZone('UTC');
    }

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DateTimeInterface);

        $days = $this->days->of((int) $value->format('Y'), (int) $value->format('n'), (int) $value->format('j'));

        if ($days < -2_147_483_648 || $days > 2_147_483_647) {
            throw new CastingException($value, type_date(), reason: 'outside the day range of a date column');
        }

        return $days;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_int($physical));

        return (new DateTimeImmutable('@' . ($physical * 86_400)))->setTimezone($this->utc);
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
            $values[] = (new DateTimeImmutable('@' . ($physical * 86_400)))->setTimezone($this->utc);
        }

        return $values;
    }
}
