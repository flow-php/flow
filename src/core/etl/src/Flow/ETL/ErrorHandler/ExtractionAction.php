<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

enum ExtractionAction
{
    case propagate;
    case endSource;
}
