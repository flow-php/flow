<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\Type;
use Flow\ETL\Row\EntryConverter;

/**
 * @implements EntryConverter<array{type: Type}>
 * @psalm-immutable
 */
final class ArrayToListCaster implements EntryConverter
{
    public function __construct(private readonly Type $type)
    {
    }

    public function __serialize() : array
    {
        return ['type' => $this->type];
    }

    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new Entry\ListEntry(
            $entry->name(),
            $this->type,
            (array) $entry->value()
        );
    }
}
