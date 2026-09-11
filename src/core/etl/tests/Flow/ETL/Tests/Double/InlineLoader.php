<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final readonly class InlineLoader implements Loader
{
    /**
     * @param callable(Rows, FlowContext): void $callback
     */
    public function __construct(
        private mixed $callback,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        ($this->callback)($rows, $context);
    }
}
