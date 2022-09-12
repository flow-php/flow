<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter;

use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 *
 * @psalm-immutable
 */
interface Filter extends Serializable
{
    public function keep(Row $row) : bool;
}
