<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum IdentityGeneration : string
{
    case ALWAYS = 'a';
    case BY_DEFAULT = 'd';
}
