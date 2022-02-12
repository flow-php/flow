<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipe;

interface Loader extends Pipe
{
    public function load(Rows $rows) : void;
}
