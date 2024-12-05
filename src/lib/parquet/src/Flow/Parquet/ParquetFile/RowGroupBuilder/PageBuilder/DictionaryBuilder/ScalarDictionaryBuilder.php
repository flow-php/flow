<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\Dictionary;

final class ScalarDictionaryBuilder
{
    /**
     * @psalm-suppress InvalidArrayOffset
     */
    public function build(FlatColumnValues $data) : Dictionary
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
