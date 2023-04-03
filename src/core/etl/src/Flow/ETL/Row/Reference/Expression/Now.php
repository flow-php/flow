<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Now implements Expression
{
    public function __construct(private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC'))
    {
    }

    public function eval(Row $row) : mixed
    {
        return new \DateTimeImmutable('now', $this->timeZone);
    }
}
