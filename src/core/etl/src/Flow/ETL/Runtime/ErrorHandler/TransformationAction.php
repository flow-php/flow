<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

enum TransformationAction
{
    case propagate;
    case skipBatch;
}
