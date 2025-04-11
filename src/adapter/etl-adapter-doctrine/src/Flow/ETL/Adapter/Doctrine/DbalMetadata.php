<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Schema\Metadata;

enum DbalMetadata : string
{
    case COLUMN_DEFINITION = 'dbal_column_column_definition';
    case COMMENT = 'dbal_column_comment';
    case CUSTOM_SCHEMA_OPTIONS = 'cdbal_column_ustom_schema_options';
    case DEFAULT = 'dbal_column_default';
    case FIXED = 'dbal_column_fixed';
    case INDEX = 'dbal_column_index';
    case INDEX_UNIQUE = 'dbal_column_index_unique';
    case LENGTH = 'dbal_column_length';
    case PLATFORM_OPTIONS = 'dbal_column_platform_options';
    case PRECISION = 'dbal_column_precision';
    case PRIMARY_KEY = 'dbal_column_primary';
    case SCALE = 'dbal_column_scale';
    case TYPE = 'dbal_column_type';
    case UNSIGNED = 'dbal_column_unsigned';

    public static function columnDefinition(string $definition) : Metadata
    {
        return Metadata::with(self::COLUMN_DEFINITION->value, $definition);
    }

    public static function comment(string|int $comment) : Metadata
    {
        return Metadata::with(self::COMMENT->value, $comment);
    }

    public static function default(int|string|bool|float $value) : Metadata
    {
        return Metadata::with(self::DEFAULT->value, $value);
    }

    public static function fixed(bool $fixed = true) : Metadata
    {
        return Metadata::with(self::FIXED->value, $fixed);
    }

    public static function index(string $name) : Metadata
    {
        return Metadata::with(self::INDEX->value, $name);
    }

    public static function indexUnique(string $name) : Metadata
    {
        return Metadata::with(self::INDEX_UNIQUE->value, $name);
    }

    public static function length(int $length) : Metadata
    {
        return Metadata::with(self::LENGTH->value, $length);
    }

    public static function platformOptions(array $options) : Metadata
    {
        return Metadata::with(self::PLATFORM_OPTIONS->value, $options);
    }

    public static function precision(int $precision) : Metadata
    {
        return Metadata::with(self::PRECISION->value, $precision);
    }

    public static function primaryKey(string $name = '') : Metadata
    {
        return Metadata::with(self::PRIMARY_KEY->value, $name);
    }

    public static function scale(int $scale) : Metadata
    {
        return Metadata::with(self::SCALE->value, $scale);
    }

    public static function type(string $type) : Metadata
    {
        return Metadata::with(self::TYPE->value, $type);
    }

    public static function unsigned(bool $unsigned = true) : Metadata
    {
        return Metadata::with(self::UNSIGNED->value, $unsigned);
    }
}
