<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use Flow\ETL\Schema\Metadata;

enum SealMetadata: string
{
    case DISTINCT = 'seal_field_distinct';
    case FACET = 'seal_field_facet';
    case FILTERABLE = 'seal_field_filterable';
    case IDENTIFIER = 'seal_field_identifier';
    case SEARCHABLE = 'seal_field_searchable';
    case SORTABLE = 'seal_field_sortable';

    public static function distinct(bool $value = true): Metadata
    {
        return Metadata::with(self::DISTINCT->value, $value);
    }

    public static function facet(bool $value = true): Metadata
    {
        return Metadata::with(self::FACET->value, $value);
    }

    public static function filterable(bool $value = true): Metadata
    {
        return Metadata::with(self::FILTERABLE->value, $value);
    }

    public static function identifier(): Metadata
    {
        return Metadata::with(self::IDENTIFIER->value, true);
    }

    public static function searchable(bool $value = true): Metadata
    {
        return Metadata::with(self::SEARCHABLE->value, $value);
    }

    public static function sortable(bool $value = true): Metadata
    {
        return Metadata::with(self::SORTABLE->value, $value);
    }
}
