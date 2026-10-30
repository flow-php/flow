<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\LoadingAction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationAction;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use SensitiveParameter;

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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
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
