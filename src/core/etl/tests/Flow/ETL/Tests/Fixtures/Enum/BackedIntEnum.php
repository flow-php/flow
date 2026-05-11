<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures\Enum;

enum BackedIntEnum: int
{
    case one = 1;
    case three = 3;
    case two = 2;
}
