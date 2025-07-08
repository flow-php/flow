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

            if (!isset($valueToIndex[$value])) {
                $dictionary[] = $value;
                $valueToIndex[$value] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$value];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = $value;
        }

        return new Dictionary($dictionary, $indices);
    }
}
