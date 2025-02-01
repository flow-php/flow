<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Row\Schema\Metadata;

enum DbalMetadata : string
{
    case COLUMN_DEFINITION = 'column_definition';
    case COMMENT = 'comment';
    case CUSTOM_SCHEMA_OPTIONS = 'custom_schema_options';
    case DEFAULT = 'default';
    case FIXED = 'fixed';
    case INDEX = 'index';
    case INDEX_UNIQUE = 'index_unique';
    case LENGTH = 'length';
    case PLATFORM_OPTIONS = 'platform_options';
    case PRECISION = 'precision';
    case PRIMARY_KEY = 'primary';
    case SCALE = 'scale';
    case UNSIGNED = 'unsigned';

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

    public static function unsigned(bool $unsigned = true) : Metadata
    {
        return Metadata::with(self::UNSIGNED->value, $unsigned);
    }
}
