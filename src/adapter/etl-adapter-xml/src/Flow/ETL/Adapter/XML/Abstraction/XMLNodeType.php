<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Abstraction;

enum XMLNodeType
{
    case FLAT;
    case NESTED;
}
