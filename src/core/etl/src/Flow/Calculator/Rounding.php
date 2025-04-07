<?php

declare(strict_types=1);

namespace Flow\Calculator;

enum Rounding
{
    case CEILING;
    case DOWN;
    case FLOOR;
    case HALF_CEILING;
    case HALF_DOWN;
    case HALF_EVEN;
    case HALF_FLOOR;
    case HALF_UP;
    case UNNECESSARY;
    case UP;
}
