<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

enum LoadingAction
{
    case propagate;
    case skipLoader;
}
