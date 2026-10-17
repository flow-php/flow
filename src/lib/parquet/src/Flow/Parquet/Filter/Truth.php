<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

enum Truth
{
    case Always;
    case Maybe;
    case Never;
}
