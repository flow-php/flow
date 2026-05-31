<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

enum SchemaObjectType: string
{
    case SCHEMA = 'schema';
    case TABLE = 'table';
    case VIEW = 'view';
    case MATERIALIZED_VIEW = 'materialized_view';
    case SEQUENCE = 'sequence';
    case FUNCTION = 'function';
    case PROCEDURE = 'procedure';
    case DOMAIN = 'domain';
    case EXTENSION = 'extension';
}
