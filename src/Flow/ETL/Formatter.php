<?php declare(strict_types=1);

namespace Flow\ETL;

interface Formatter
{
    public function format(Rows $rows, int $truncate = 20) : string;
}
