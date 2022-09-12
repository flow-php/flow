<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToBooleanCaster;

/**
 * @implements EntryConverter<array{value_caster: AnyToBooleanCaster}>
 *
 * @psalm-immutable
 */
final class AnyToBooleanEntryCaster implements EntryConverter
{
    private AnyToBooleanCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToBooleanCaster();
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
        return new BooleanEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}
