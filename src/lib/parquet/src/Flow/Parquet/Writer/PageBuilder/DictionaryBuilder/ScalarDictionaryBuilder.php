<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

final class ScalarDictionaryBuilder
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

            $key = \is_float($value) ? \serialize($value) : $value;

            if (!isset($valueToIndex[$key])) {
                $dictionary[] = $value;
                $valueToIndex[$key] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$key];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = $value;
        }

        return new Dictionary($dictionary, $indices);
    }
}
