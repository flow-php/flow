<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Rows;

interface JSONEncoder
{
    public function encode(Rows $rows, string $separator): string;
}
