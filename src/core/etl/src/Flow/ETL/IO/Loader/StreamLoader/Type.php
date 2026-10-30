<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\StreamLoader;

enum Type
{
    case custom;
    case output;
    case stderr;
    case stdout;
}
