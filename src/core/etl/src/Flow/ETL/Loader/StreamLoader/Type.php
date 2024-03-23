<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\StreamLoader;

enum Type
{
    case custom;
    case output;
    case stderr;
    case stdout;
}
