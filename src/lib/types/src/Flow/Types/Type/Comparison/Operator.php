<?php

declare(strict_types=1);

namespace Flow\Types\Type\Comparison;

enum Operator : string
{
    case DIFFERENT = '<>';
    case EQUAL = '==';
    case GREATER_THAN = '>';
    case GREATER_THAN_EQUAL = '>=';
    case IDENTICAL = '===';
    case LESS_THAN = '<';
    case LESS_THAN_EQUAL = '<=';
    case NOT_EQUAL = '!=';
    case NOT_IDENTICAL = '!==';
    case SPACE_SHIP = '<=>';
}
