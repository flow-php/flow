<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;

final class RecordingErrorHandler implements ErrorHandler
{
    /**
     * @var list<ExtractionError|LoadingError|TransformationError>
     */
    public array $errors = [];

    public function __construct(
        private readonly ErrorHandler $decides = new IgnoreError(),
    ) {}

    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        $this->errors[] = $error;

        return $this->decides->onExtraction($error);
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        $this->errors[] = $error;

        return $this->decides->onLoading($error);
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        $this->errors[] = $error;

        return $this->decides->onTransformation($error);
    }
}
