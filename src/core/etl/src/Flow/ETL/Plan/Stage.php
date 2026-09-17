<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum Stage
{
    case unoptimized;
    case optimized;
}
