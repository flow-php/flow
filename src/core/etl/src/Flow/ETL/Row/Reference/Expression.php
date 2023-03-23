<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use Flow\ETL\Row;

interface Expression
{
    public function eval(Row $row) : mixed;
}
