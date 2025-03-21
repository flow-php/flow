<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation\AddRowIndex;

enum StartFrom
{
    case ONE;
    case ZERO;
}
