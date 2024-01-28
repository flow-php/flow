<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Fixtures;

enum SomeEnum : string
{
    case A = 'a';
    case B = 'b';
    case C = 'c';
}
