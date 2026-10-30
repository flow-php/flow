<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\LoadingAction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationAction;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;

interface ErrorHandler extends Describable
{
    public function onExtraction(ExtractionError $error): ExtractionAction;

    public function onTransformation(TransformationError $error): TransformationAction;

    public function onLoading(LoadingError $error): LoadingAction;
}
