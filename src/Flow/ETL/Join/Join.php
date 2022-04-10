<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

enum Join
{
    case inner;
    case left;
    case right;
}
