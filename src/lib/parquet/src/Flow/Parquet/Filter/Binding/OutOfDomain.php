<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

/**
 * An edge below or above every physical value of the column.
 */
enum OutOfDomain
{
    case Above;
    case Below;
}
