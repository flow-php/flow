<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class CallbackLoader implements Loader
{
    /**
     * @phpstan-ignore-next-line
     *
     * @param callable(Rows $row) : void $callback
     */
    private $callback;

    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }

    public function load(Rows $rows) : void
    {
        ($this->callback)($rows);
    }
}
