<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Converter;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Converter;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\JsonEntry;

/**
 * @psalm-immutable
 */
final class ToJsonEntry implements Converter
{
    public function convert(Entry $entry) : Entry
    {
        if (!$entry instanceof CollectionEntry) {
            throw RuntimeException::because(
                'Only "%s" can be transformed to "%s", but "%s" is a "%s"',
                CollectionEntry::class,
                JsonEntry::class,
                $entry->name(),
                \get_class($entry)
            );
        }

        return new JsonEntry($entry->name(), $entry->value());
    }
}
