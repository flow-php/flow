<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;

final class SchemaConverter
{
    /**
     * @return array<array<string, mixed>>
     */
    public static function toExtension(Schema $schema): array
    {
        $result = [];

        foreach ($schema->columns() as $column) {
            $result[] = self::columnToExtension($column);
        }

        return $result;
    }

    /**
     * @return array<string, mixed>
     */
    private static function columnToExtension(Column $column): array
    {
        if ($column instanceof FlatColumn) {
            return self::flatColumnToExtension($column);
        }

        if ($column instanceof NestedColumn) {
            return self::nestedColumnToExtension($column);
        }

        throw new InvalidArgumentException('Unknown column type: ' . $column::class);
    }

    /**
     * @return array<string, mixed>
     */
    private static function flatColumnToExtension(FlatColumn $column): array
    {
        $entry = [
            'name' => $column->name(),
            'type' => self::resolveFlatType($column),
            'optional' => $column->repetition() === Repetition::OPTIONAL,
        ];

        if (
            $column->logicalType()?->name() === LogicalType::DECIMAL
            || $column->convertedType() === ConvertedType::DECIMAL
        ) {
            $entry['precision'] = $column->precision();
            $entry['scale'] = $column->scale();
        }

        if (
            $column->type() === PhysicalType::FIXED_LEN_BYTE_ARRAY
            && $column->logicalType()?->name() !== LogicalType::DECIMAL
            && $column->logicalType()?->name() !== LogicalType::UUID
        ) {
            $entry['length'] = $column->typeLength();
        }

        return $entry;
    }

    /**
     * @return array<string, mixed>
     */
    private static function nestedColumnToExtension(NestedColumn $column): array
    {
        $entry = [
            'name' => $column->name(),
            'optional' => $column->repetition() === Repetition::OPTIONAL,
        ];

        if ($column->isList()) {
            $entry['type'] = 'LIST';
            $entry['children'] = [self::columnToExtension($column->getListElement())];
        } elseif ($column->isMap()) {
            $entry['type'] = 'MAP';
            $mapValueColumn = $column->getMapValueColumn();
            $entry['children'] = $mapValueColumn !== null
                ? [self::columnToExtension($column->getMapKeyColumn()), self::columnToExtension($mapValueColumn)]
                : [self::columnToExtension($column->getMapKeyColumn())];
        } else {
            $entry['type'] = 'STRUCT';
            $entry['children'] = [];

            foreach ($column->children() as $child) {
                $entry['children'][] = self::columnToExtension($child);
            }
        }

        return $entry;
    }

    private static function resolveFlatType(FlatColumn $column): string
    {
        $logicalName = $column->logicalType()?->name();

        if ($logicalName !== null) {
            return match ($logicalName) {
                LogicalType::STRING => 'STRING',
                LogicalType::DATE => 'DATE',
                LogicalType::TIMESTAMP => 'TIMESTAMP',
                LogicalType::TIME => 'TIME',
                LogicalType::DECIMAL => 'DECIMAL',
                LogicalType::UUID => 'UUID',
                LogicalType::JSON => 'JSON',
                LogicalType::ENUM => 'STRING',
                LogicalType::BSON => 'BINARY',
                LogicalType::INTEGER => self::resolveIntegerType($column),
                default => self::resolveFromConvertedOrPhysical($column),
            };
        }

        return self::resolveFromConvertedOrPhysical($column);
    }

    private static function resolveFromConvertedOrPhysical(FlatColumn $column): string
    {
        $convertedType = $column->convertedType();

        if ($convertedType !== null) {
            return match ($convertedType) {
                ConvertedType::UTF8 => 'STRING',
                ConvertedType::DATE => 'DATE',
                ConvertedType::TIMESTAMP_MILLIS, ConvertedType::TIMESTAMP_MICROS => 'TIMESTAMP',
                ConvertedType::TIME_MILLIS, ConvertedType::TIME_MICROS => 'TIME',
                ConvertedType::DECIMAL => 'DECIMAL',
                ConvertedType::JSON => 'JSON',
                ConvertedType::ENUM => 'STRING',
                ConvertedType::BSON => 'BINARY',
                ConvertedType::INT_8 => 'INT8',
                ConvertedType::INT_16 => 'INT16',
                ConvertedType::INT_32 => 'INT32',
                ConvertedType::INT_64 => 'INT64',
                ConvertedType::UINT_8 => 'UINT8',
                ConvertedType::UINT_16 => 'UINT16',
                ConvertedType::UINT_32 => 'UINT32',
                ConvertedType::UINT_64 => 'UINT64',
                default => self::resolveFromPhysicalType($column),
            };
        }

        return self::resolveFromPhysicalType($column);
    }

    private static function resolveFromPhysicalType(FlatColumn $column): string
    {
        return match ($column->type()) {
            PhysicalType::BOOLEAN => 'BOOLEAN',
            PhysicalType::INT32 => 'INT32',
            PhysicalType::INT64 => 'INT64',
            PhysicalType::INT96 => 'INT64',
            PhysicalType::FLOAT => 'FLOAT',
            PhysicalType::DOUBLE => 'DOUBLE',
            PhysicalType::BYTE_ARRAY => 'BINARY',
            PhysicalType::FIXED_LEN_BYTE_ARRAY => 'FIXED_SIZE_BINARY',
        };
    }

    private static function resolveIntegerType(FlatColumn $column): string
    {
        return match ($column->convertedType()) {
            ConvertedType::INT_8 => 'INT8',
            ConvertedType::INT_16 => 'INT16',
            ConvertedType::INT_32 => 'INT32',
            ConvertedType::INT_64 => 'INT64',
            ConvertedType::UINT_8 => 'UINT8',
            ConvertedType::UINT_16 => 'UINT16',
            ConvertedType::UINT_32 => 'UINT32',
            ConvertedType::UINT_64 => 'UINT64',
            default => match ($column->type()) {
                PhysicalType::INT32 => 'INT32',
                PhysicalType::INT64 => 'INT64',
                default => 'INT64',
            },
        };
    }
}
