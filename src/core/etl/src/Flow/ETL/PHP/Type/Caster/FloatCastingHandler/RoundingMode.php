<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster\FloatCastingHandler;

enum RoundingMode : int
{
    case ROUND_HALF_DOWN = 2;
    case ROUND_HALF_EVEN = 3;
    case ROUND_HALF_ODD = 4;
    case ROUND_HALF_UP = 1;
}
