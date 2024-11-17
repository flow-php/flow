<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{FlowContext, Loader, Rows};

final class CallbackLoader implements Loader
{
    /**
     * @param callable(Rows $row, FlowContext $context) : void $callback
     *
     * @phpstan-ignore-next-line
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
