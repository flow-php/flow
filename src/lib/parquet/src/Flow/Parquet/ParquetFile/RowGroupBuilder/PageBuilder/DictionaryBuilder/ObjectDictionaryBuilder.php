<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\Dictionary;

final class ObjectDictionaryBuilder
{
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

            $hash = \serialize($value);

            if (!isset($valueToIndex[$hash])) {
                $dictionary[] = $hash;
                $valueToIndex[$hash] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$hash];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = @\unserialize($value, ['allowed_classes' => [\DateTimeImmutable::class, \DateInterval::class]]);
        }

        return new Dictionary($dictionary, $indices);
    }
}
