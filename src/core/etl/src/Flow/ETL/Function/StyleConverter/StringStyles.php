<?php

declare(strict_types=1);

namespace Flow\ETL\Function\StyleConverter;

/**
 * @deprecated use Flow\ETL\String\String
 */
enum StringStyles : string
{
    case ASCII = 'ascii';
    case CAMEL = 'camel';
    case KEBAB = 'kebab';
    case LOWER = 'lower';
    case SLUG = 'slug';
    case SNAKE = 'snake';
    case TITLE = 'title';
    case UCFIRST = 'ucfirst';
    case UCWORDS = 'ucwords';
    case UPPER = 'upper';
}
