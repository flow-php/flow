<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Loader;

enum XMLNodeType
{
    case FLAT;
    case NESTED;
}
