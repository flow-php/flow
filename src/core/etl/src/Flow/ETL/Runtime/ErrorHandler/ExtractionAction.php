<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

enum ExtractionAction
{
    case propagate;
    case endSource;
}
