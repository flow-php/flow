<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

enum SortOrder : string
{
    case ASC = 'ASC';
    case DESC = 'DESC';
}
