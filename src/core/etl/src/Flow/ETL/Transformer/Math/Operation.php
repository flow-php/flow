<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Math;

enum Operation: string
{
    case add = 'add';
    case divide = 'divide';
    case modulo = 'modulo';
    case multiply = 'multiply';
    case power = 'power';
    case subtract = 'subtract';
}
