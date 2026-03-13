<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Binary\Bytes;
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

            if ($value instanceof Bytes) {
                $key = $value->toString();
            } elseif (\is_float($value)) {
                $key = \serialize($value);
            } else {
                $key = $value;
            }

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
