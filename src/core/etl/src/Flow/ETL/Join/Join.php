<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

enum Join : string
{
    case inner = 'inner';
    case left = 'left';
    case left_anti = 'left_anti';
    case right = 'right';
}
