<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface CastRow extends Serializable
{
    public function cast(Row $row) : Row;
}
