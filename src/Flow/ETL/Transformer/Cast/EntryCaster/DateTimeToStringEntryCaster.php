<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\DateTimeToStringCaster;

/**
 * @psalm-immutable
 */
final class DateTimeToStringEntryCaster implements EntryConverter
{
    private DateTimeToStringCaster $valueCaster;

    public function __construct(string $format = \DateTimeInterface::ATOM)
    {
        $this->valueCaster = new DateTimeToStringCaster($format);
    }

    /**
     * @return array{value_caster: DateTimeToStringCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: DateTimeToStringCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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
