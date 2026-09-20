<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class CallOrderLoader implements Loader
{
    /**
     * @param ArrayObject<int, string> $log shared by every loader whose call order a test compares
     */
    public function __construct(
        private readonly string $name,
        private readonly ArrayObject $log,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->log[] = "{$this->name}:{$rows->count()}";
    }
}
