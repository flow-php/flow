<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;

final readonly class PropagatingTransformationsSkippingLoaders implements ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::propagate;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return LoadingAction::skipLoader;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return TransformationAction::propagate;
    }
}
