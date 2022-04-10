<?php declare(strict_types=1);

namespace Flow\ETL;

interface Formatter
{
    public function format(Rows $rows, int|bool $truncate = 20) : string;
}
