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
final class AnyToStringEntryCaster implements EntryCaster
{
    private ValueCaster\AnyToStringCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new ValueCaster\AnyToStringCaster();
    }

    /**
     * @return array{value_caster: ValueCaster\AnyToStringCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: ValueCaster\AnyToStringCaster} $data
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
