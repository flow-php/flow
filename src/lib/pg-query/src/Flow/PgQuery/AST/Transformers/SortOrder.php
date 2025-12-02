<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

enum SortOrder : string
{
    case ASC = 'ASC';
    case DESC = 'DESC';
}
