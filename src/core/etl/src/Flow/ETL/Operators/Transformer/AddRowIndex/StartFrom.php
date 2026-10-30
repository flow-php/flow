<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer\AddRowIndex;

enum StartFrom
{
    case ONE;
    case ZERO;
}
