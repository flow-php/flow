<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum DefaultKind: string
{
    case CONSTANT = 'constant';
    case EXPRESSION = 'expression';
}
