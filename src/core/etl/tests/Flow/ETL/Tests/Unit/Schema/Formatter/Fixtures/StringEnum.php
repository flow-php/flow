<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter\Fixtures;

enum StringEnum : string
{
    case A = 'a';
    case B = 'b';
    case C = 'c';
}
