<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipe;

/**
 * @template T
 * @extends Pipe<T>
 */
interface Loader extends Pipe
{
    public function load(Rows $rows) : void;
}
