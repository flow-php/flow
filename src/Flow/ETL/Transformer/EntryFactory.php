<?php declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row\Entry;
use Flow\Serializer\Serializable;

interface EntryFactory extends Serializable
{
    /**
     * @param string $entryName
     * @param mixed $value
     *
     * @return Entry
     */
    public function createEntry(string $entryName, $value) : Entry;
}
