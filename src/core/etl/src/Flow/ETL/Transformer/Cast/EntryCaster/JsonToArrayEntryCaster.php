<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\JsonToArrayCaster;

/**
 * @implements EntryConverter<array{value_caster: JsonToArrayCaster}>
 * @psalm-immutable
 */
final class JsonToArrayEntryCaster implements EntryConverter
{
    private JsonToArrayCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new JsonToArrayCaster();
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
        return new ArrayEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}
