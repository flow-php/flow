<?php

declare(strict_types=1);

namespace Flow\ETL\Function\DOM;

enum ElementSibling : string
{
    case NEXT = 'next';
    case PREVIOUS = 'previous';
}
