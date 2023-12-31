<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class CallbackLoader implements Loader
{
    /**
     * @phpstan-ignore-next-line
     *
     * @param callable(Rows $row, FlowContext $context) : void $callback
     */
    private $callback;

    public function __construct(callable $callback)
    {
        $this->callback = $callback;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        ($this->callback)($rows, $context);
    }
}
