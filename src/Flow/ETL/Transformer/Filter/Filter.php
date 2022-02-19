<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter;

use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface Filter extends Serializable
{
    public function keep(Row $row) : bool;
}
