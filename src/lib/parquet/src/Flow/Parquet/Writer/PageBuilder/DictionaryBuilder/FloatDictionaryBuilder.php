<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

final class FloatDictionaryBuilder
{
    public function build(WriteFlatColumnValues $data) : Dictionary
    {
        $dictionary = [];
        $indices = [];
        $valueToIndex = [];
        $dictionarySize = 0;

        foreach ($data->values() as $value) {
            if ($value === null) {
                continue;
            }

            $hash = \serialize($value);

            if (!isset($valueToIndex[$hash])) {
                $dictionary[] = $hash;
                $valueToIndex[$hash] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$hash];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = @\unserialize($value, ['allowed_classes' => []]);
        }

        return new Dictionary($dictionary, $indices);
    }
}
