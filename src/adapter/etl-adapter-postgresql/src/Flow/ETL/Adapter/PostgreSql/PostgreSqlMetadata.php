<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Metadata;
use Flow\PostgreSql\Schema\IdentityGeneration;

use function str_contains;

/**
 * Per-column metadata keys understood by {@see SchemaConverter} when converting a Flow Schema to a
 * PostgreSQL table.
 */
enum PostgreSqlMetadata: string
{
    case DEFAULT = 'pgsql_column_default';
    case GENERATED = 'pgsql_column_generated';
    case IDENTITY = 'pgsql_column_identity';
    case INDEX = 'pgsql_column_index';
    case INDEX_UNIQUE = 'pgsql_column_index_unique';
    case LENGTH = 'pgsql_column_length';
    case PRECISION = 'pgsql_column_precision';
    case PRIMARY_KEY = 'pgsql_column_primary';
    case SCALE = 'pgsql_column_scale';
    case TYPE = 'pgsql_column_type';

    public static function default(bool|float|int|string $value): Metadata
    {
        return Metadata::with(self::DEFAULT->value, $value);
    }

    public static function generated(string $expression): Metadata
    {
        return Metadata::with(self::GENERATED->value, $expression);
    }

    public static function identity(IdentityGeneration $generation = IdentityGeneration::ALWAYS): Metadata
    {
        return Metadata::with(self::IDENTITY->value, $generation->value);
    }

    public static function index(string $name, int $position = PHP_INT_MAX): Metadata
    {
        if (str_contains($name, ':')) {
            throw InvalidArgumentException::because('PostgreSQL index name "%s" must not contain a colon ":"', $name);
        }

        return Metadata::with(self::INDEX->value . ':' . $name, $position);
    }

    public static function indexUnique(string $name, int $position = PHP_INT_MAX): Metadata
    {
        if (str_contains($name, ':')) {
            throw InvalidArgumentException::because(
                'PostgreSQL unique index name "%s" must not contain a colon ":"',
                $name,
            );
        }

        return Metadata::with(self::INDEX_UNIQUE->value . ':' . $name, $position);
    }

    public static function length(int $length): Metadata
    {
        return Metadata::with(self::LENGTH->value, $length);
    }

    public static function precision(int $precision): Metadata
    {
        return Metadata::with(self::PRECISION->value, $precision);
    }

    public static function primaryKey(string $name = ''): Metadata
    {
        return Metadata::with(self::PRIMARY_KEY->value, $name);
    }

    public static function scale(int $scale): Metadata
    {
        return Metadata::with(self::SCALE->value, $scale);
    }

    public static function type(string $type): Metadata
    {
        return Metadata::with(self::TYPE->value, $type);
    }
}
