<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster\Fixtures;

enum ColorsEnum : string
{
    case BLUE = 'blue';
    case GREEN = 'green';
    case RED = 'red';
}
