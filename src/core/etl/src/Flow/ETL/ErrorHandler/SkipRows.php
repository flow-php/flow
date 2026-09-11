<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\ErrorHandler;

final class SkipRows implements ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::endSource;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return LoadingAction::propagate;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return TransformationAction::skipBatch;
    }
}
