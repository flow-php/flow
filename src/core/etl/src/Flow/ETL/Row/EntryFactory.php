<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface EntryFactory extends Serializable
{
    public function create(string $entryName, mixed $value, ?Schema $schema = null) : Entry;
}
