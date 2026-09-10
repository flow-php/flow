<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

enum LoadingAction
{
    case propagate;
    case skipLoader;
}
