<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

/**
 * @implements Constraint<array<mixed>>
 */
final class NotEmpty implements Constraint
{
    public function __construct()
    {
    }

    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function isSatisfiedBy(Entry $entry) : bool
    {
        return match (\get_class($entry)) {
            Entry\ArrayEntry::class,
            Entry\CollectionEntry::class,
            Entry\StructureEntry::class,
            Entry\ListEntry::class => (bool) \count($entry->value()),
            Entry\StringEntry::class => $entry->value() !== '',
            Entry\JsonEntry::class => !\in_array($entry->value(), ['', '[]', '{}'], true),
            default => true, //everything else can't be empty
        };
    }
}
