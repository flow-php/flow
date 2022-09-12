<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToJsonCaster;

/**
 * @implements EntryConverter<array{value_caster: AnyToJsonCaster}>
 *
 * @psalm-immutable
 */
final class AnyToJsonEntryCaster implements EntryConverter
{
    private AnyToJsonCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToJsonCaster();
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
        return new JsonEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}
