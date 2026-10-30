<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination;

enum InjectInto
{
    case QueryParam;
    case Header;
    case BodyPath;
    case ReplaceUri;
}
