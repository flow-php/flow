<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipe;

/**
 * @psalm-immutable
 */
interface Transformer extends Pipe
{
    public function transform(Rows $rows) : Rows;
}
