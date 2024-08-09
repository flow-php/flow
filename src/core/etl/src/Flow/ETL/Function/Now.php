<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Now extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC'))
    {
    }

    public function eval(Row $row) : ?\DateTimeImmutable
    {
        $tz = (new Parameter($this->timeZone))->asInstanceOf($row, \DateTimeZone::class);

        if ($tz === null) {
            return null;
        }

        return new \DateTimeImmutable('now', $tz);
    }
}
