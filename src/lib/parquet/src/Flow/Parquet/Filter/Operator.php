<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

enum Operator
{
    case Equal;
    case GreaterThan;
    case GreaterThanOrEqual;
    case LessThan;
    case LessThanOrEqual;
}
