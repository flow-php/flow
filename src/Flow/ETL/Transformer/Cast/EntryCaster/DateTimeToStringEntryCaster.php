<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Transformer\Cast\EntryCaster;
use Flow\ETL\Transformer\Cast\ValueCaster;

/**
 * @psalm-immutable
 */
final class DateTimeToStringEntryCaster implements EntryCaster
{
    private ValueCaster\DateTimeToStringCaster $valueCaster;

    public function __construct(string $format = \DateTimeInterface::ATOM)
    {
        $this->valueCaster = new ValueCaster\DateTimeToStringCaster($format);
    }

    /**
     * @return array{value_caster: ValueCaster\DateTimeToStringCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: ValueCaster\DateTimeToStringCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function cast(Entry $entry) : Entry
    {
        return new StringEntry(
            $entry->name(),
            $this->valueCaster->cast($entry->value())
        );
    }
}
