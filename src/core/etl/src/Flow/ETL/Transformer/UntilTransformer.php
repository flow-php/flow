<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class UntilTransformer implements Transformer
{
    private bool $limitReached = false;

    public function __construct(
        private readonly ScalarFunction $function,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            if ($this->limitReached) {
                $context->telemetry()->transformationCompleted($this, [
                    TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                    TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => 0,
                ]);

                throw new LimitReachedException(0);
            }

            $nextRows = [];

            foreach ($rows as $row) {
                if (!$this->function->eval($row, $context)) {
                    $this->limitReached = true;
                } else {
                    $nextRows[] = $row;
                }
            }

            $result = new Rows(...$nextRows);

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (LimitReachedException $e) {
            throw $e;
        } catch (\Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
