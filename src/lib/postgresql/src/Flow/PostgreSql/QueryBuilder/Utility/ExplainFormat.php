<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

enum ExplainFormat: string
{
    case JSON = 'json';
    case TEXT = 'text';
    case XML = 'xml';
    case YAML = 'yaml';
}
