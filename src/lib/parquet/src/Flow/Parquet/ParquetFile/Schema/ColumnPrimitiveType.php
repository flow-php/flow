<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

final class ColumnPrimitiveType
{
    public static function isString(FlatColumn $column) : bool
    {
        $logicalType = $column->logicalType();

        if ($logicalType === null) {
            if ($column->convertedType() === ConvertedType::UTF8) {
                return true;
            }

            return false;
        }

        return \in_array($logicalType->name(), [LogicalType::STRING, LogicalType::UUID, LogicalType::ENUM, LogicalType::JSON], true);
    }
}
