<?php

declare(strict_types=1);

namespace Flow\ETL\IO;

use Flow\ETL\Values\Rows;

interface Formatter
{
    public function format(Rows $rows, int|bool $truncate = 20): string;
}
