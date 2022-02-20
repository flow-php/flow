<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

interface EntryFactory extends Serializable
{
    /**
     * @param string $entryName
     * @param mixed $value
     *
     * @return Entry
     */
    public function create(string $entryName, $value) : Entry;
}
