<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final readonly class ValueConverters
{
    public static function for(Column $column): ?ValueConverter
    {
        if ($column instanceof FlatColumn) {
            return match ($column->logicalType()?->name()) {
                LogicalType::JSON => new JsonValueConverter(),
                LogicalType::UUID => new UuidValueConverter(),
                default => null,
            };
        }

        /** @var NestedColumn $column */
        if ($column->isList()) {
            $element = self::for($column->getListElement());

            return $element === null ? null : new ElementsValueConverter($element);
        }

        if ($column->isMap()) {
            $valueColumn = $column->getMapValueColumn();
            $value = $valueColumn === null ? null : self::for($valueColumn);

            return $value === null ? null : new ElementsValueConverter($value);
        }

        $children = [];

        foreach ($column->children() as $child) {
            $childConverter = self::for($child);

            if ($childConverter !== null) {
                $children[$child->name()] = $childConverter;
            }
        }

        return $children === [] ? null : new StructValueConverter($children);
    }
}
