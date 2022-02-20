<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\StringToDateTimeCaster;

/**
 * @psalm-immutable
 */
final class StringToDateTimeEntryCaster implements EntryConverter
{
    private StringToDateTimeCaster $valueCaster;

    /**
     * $timezone - this value should be used for datetime values that does not come with explicit tz to avoid using system default.
     * For example when the datetime is "2020-01-01 00:00:00" and we know that it's utc, then $timeZone should be set to 'UTC'.
     *
     * $toTimeZone - this value should be used to convert datetime to different timezone. So when the datetime comes in one timezone
     * "2020-01-01 00:00:00 UTC" and we want to convert it to America/Los_Angeles use $toTimeZone = 'America/Los_Angeles".
     * If datetime comes without origin timezone, like for example '2020-01-01 00:00:00' but we know it's UTC
     * and we want to cast it to 'America/Los_Angeles' use $timeZone = 'UTC' and $toTimeZone = 'America/Los_Angeles'.
     *
     * @param null|string $timeZone
     * @param null|string $toTimeZone
     */
    public function __construct(?string $timeZone = null, ?string $toTimeZone = null)
    {
        $this->valueCaster = new StringToDateTimeCaster($timeZone, $toTimeZone);
    }

    /**
     * @return array{value_caster: StringToDateTimeCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: StringToDateTimeCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new DateTimeEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}
