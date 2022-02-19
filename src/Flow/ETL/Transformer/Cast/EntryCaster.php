<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row\Entry;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface EntryCaster extends Serializable
{
    public function cast(Entry $entry) : Entry;
}
