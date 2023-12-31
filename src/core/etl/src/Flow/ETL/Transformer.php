<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\LimitReachedException;

interface Transformer
{
    /**
     * @throws LimitReachedException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows;
}
