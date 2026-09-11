<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use function rtrim;

final readonly class DescribeQuery
{
    public function of(string $sql): string
    {
        return "SELECT * FROM (\n" . rtrim($sql, " \t\r\n;") . "\n) flow_describe WHERE 1=0";
    }
}
