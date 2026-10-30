<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Runtime\ErrorHandler;
use SensitiveParameter;

final class IgnoreError implements ErrorHandler
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, []);
    }

    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return ExtractionAction::endSource;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        return LoadingAction::skipLoader;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        return TransformationAction::skipBatch;
    }
}
