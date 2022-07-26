<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Fixtures\Enum;

enum BackedStringEnum : string
{
    case one = 'one';
    case three = 'three';
    case two = 'two';
}
