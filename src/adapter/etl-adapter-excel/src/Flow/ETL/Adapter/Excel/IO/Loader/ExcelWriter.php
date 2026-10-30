<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Loader;

enum ExcelWriter
{
    case ODS;
    case XLSX;
}
