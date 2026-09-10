<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

enum TransformationAction
{
    case propagate;
    case skipBatch;
}
