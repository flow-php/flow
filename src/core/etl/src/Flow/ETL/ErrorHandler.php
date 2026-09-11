<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;

interface ErrorHandler
{
    public function onExtraction(ExtractionError $error): ExtractionAction;

    public function onTransformation(TransformationError $error): TransformationAction;

    public function onLoading(LoadingError $error): LoadingAction;
}
