<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{IntegerEntry, UuidEntry};

final readonly class TypePriorities
{
    /**
     * @var array<class-string<Entry<mixed, mixed>>,int>
     */
    public const PRIORITIES = [
        UuidEntry::class => 1,
        IntegerEntry::class => 2,
        Entry\BooleanEntry::class => 3,
        Entry\FloatEntry::class => 4,
        Entry\DateTimeEntry::class => 5,
        Entry\StringEntry::class => 6,
        Entry\EnumEntry::class => 7,
        Entry\ListEntry::class => 8,
        Entry\JsonEntry::class => 9,
        Entry\MapEntry::class => 10,
        Entry\StructureEntry::class => 11,
        Entry\XMLEntry::class => 12,
        Entry\XMLElementEntry::class => 13,
    ];

    /**
     * @param array<class-string<Entry<mixed, mixed>>,int> $priorities
     */
    public function __construct(private array $priorities = self::PRIORITIES)
    {

    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function for(Entry $entry) : int
    {
        if (!\array_key_exists($entry::class, $this->priorities)) {
            return 99999;
        }

        return $this->priorities[$entry::class];
    }
}
