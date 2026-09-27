<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

final readonly class RenameEntryTransformer implements Transformer
{
    public function __construct(
        private string $from,
        private string $to,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        if ($this->from === $this->to) {
            return new BoundStep($this, $input);
        }

        return new BoundStep($this, $input->rename($this->from, $this->to));
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        if ($this->from === $this->to) {
            return $rows;
        }

        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->withSchema($rows->schema()->rename($this->from, $this->to));

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
