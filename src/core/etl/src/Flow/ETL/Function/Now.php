<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Now extends ScalarFunctionChain
{
    public function __construct(private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC'))
    {
    }

    public function eval(Row $row) : mixed
    {
        return new \DateTimeImmutable('now', $this->timeZone);
    }
}
