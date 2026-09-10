<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

enum Stage
{
    case read_select;
    case read_select_write;
}
