<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToStringCaster;

/**
 * @implements EntryConverter<array{value_caster: AnyToStringCaster}>
 * @psalm-immutable
 */
final class AnyToStringEntryCaster implements EntryConverter
{
    private AnyToStringCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToStringCaster();
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
