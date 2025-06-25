<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{BooleanEntry, DateTimeEntry, EnumEntry, FloatEntry, JsonEntry, ListEntry, MapEntry, StringEntry, StructureEntry, XMLElementEntry, XMLEntry};
use Flow\ETL\Row\Entry\{IntegerEntry, UuidEntry};

final readonly class TypePriorities
{
    /**
     * @var array<class-string<Entry<mixed>>,int>
     */
    public const PRIORITIES = [
        UuidEntry::class => 1,
        IntegerEntry::class => 2,
        BooleanEntry::class => 3,
        FloatEntry::class => 4,
        DateTimeEntry::class => 5,
        StringEntry::class => 6,
        EnumEntry::class => 7,
        ListEntry::class => 8,
        JsonEntry::class => 9,
        MapEntry::class => 10,
        StructureEntry::class => 11,
        XMLEntry::class => 12,
        XMLElementEntry::class => 13,
    ];

    /**
     * @param array<class-string<Entry<mixed>>,int> $priorities
     */
    public function __construct(private array $priorities = self::PRIORITIES)
    {

    }

    /**
     * @param Entry<mixed> $entry
     */
    public function for(Entry $entry) : int
    {
        if (!\array_key_exists($entry::class, $this->priorities)) {
            return 99999;
        }

        return $this->priorities[$entry::class];
    }
}
