<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface EntryConverter extends Serializable
{
    public function convert(Entry $entry) : Entry;
}
