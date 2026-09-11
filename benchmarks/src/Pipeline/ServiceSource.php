<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

enum ServiceSource: string
{
    case doctrine = 'doctrine';
    case postgresql = 'postgresql';
}
