<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 * @psalm-immutable
 */
interface EntryConverter extends Serializable
{
    public function convert(Entry $entry) : Entry;
}
