<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface Loader extends Serializable
{
    public function load(Rows $rows) : void;
}
