<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

interface WindowFunction
{
    public function apply(Row $row, Rows $partition, Window $windowSpec) : mixed;
}
