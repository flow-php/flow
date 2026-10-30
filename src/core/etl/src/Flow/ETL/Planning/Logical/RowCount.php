<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

enum RowCount
{
    case source;
    case preserving;
    case reducing;
    case expanding;
    case unknown;
}
