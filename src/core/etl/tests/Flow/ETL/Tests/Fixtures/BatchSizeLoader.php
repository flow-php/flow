<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\BatchLoader;
use Flow\ETL\Rows;

final class BatchSizeLoader implements BatchLoader, Loader
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
    }
}
