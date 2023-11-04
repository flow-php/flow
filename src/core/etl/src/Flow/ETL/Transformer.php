<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\LimitReachedException;
use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface Transformer extends Serializable
{
    /**
     * @throws LimitReachedException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows;
}
