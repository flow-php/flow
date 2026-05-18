<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\SchemaValidator;
use Throwable;

final readonly class SchemaValidationLoader implements Loader
{
    public function __construct(
        private Schema $expected,
        private SchemaValidator $validator,
    ) {}

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $given = $rows->schema();

            if (!$this->validator->isValid($this->expected, $given)) {
                throw new SchemaValidationException($this->expected, $given);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }
}
