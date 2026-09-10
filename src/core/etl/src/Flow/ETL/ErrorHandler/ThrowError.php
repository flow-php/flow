<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\ErrorHandler;

final class ThrowError implements ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::propagate;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return LoadingAction::propagate;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return TransformationAction::propagate;
    }
}
