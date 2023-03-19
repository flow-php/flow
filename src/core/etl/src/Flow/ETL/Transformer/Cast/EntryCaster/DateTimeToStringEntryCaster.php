<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\DateTimeToStringCaster;

/**
 * @implements EntryConverter<array{value_caster: DateTimeToStringCaster}>
 */
final class DateTimeToStringEntryCaster implements EntryConverter
{
    private DateTimeToStringCaster $valueCaster;

    public function __construct(string $format = \DateTimeInterface::ATOM)
    {
        $this->valueCaster = new DateTimeToStringCaster($format);
    }

    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new StringEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}
