<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface Transformer extends Serializable
{
    public function transform(Rows $rows, FlowContext $context) : Rows;
}
