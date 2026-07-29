<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class RowsMother
{
    public static function sequentialIds(int $count): Rows
    {
        $sequence = [];

        for ($id = 1; $id <= $count; $id++) {
            $sequence[] = row(int_entry('id', $id));
        }

        return rows(...$sequence);
    }
}
