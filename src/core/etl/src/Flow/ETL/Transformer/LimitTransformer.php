<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

final class LimitTransformer implements Transformer
{
    private int $rowsCount = 0;

    public function __construct(
        public readonly int $limit,
    ) {
        if ($this->limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: " . $this->limit);
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $inputRowCount = $rows->count();

        $context->telemetry()->transformationStarted($this);

        try {
            $this->rowsCount += $rows->count();

            if ($this->rowsCount >= $this->limit) {
                $trimmed = $this->rowsCount > $this->limit ? $rows->dropRight($this->rowsCount - $this->limit) : $rows;

                $context->telemetry()->transformationCompleted($this, [
                    TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $inputRowCount,
                    TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $trimmed->count(),
                ]);

                throw new LimitReachedException($this->limit, $trimmed);
            }

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $inputRowCount,
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $rows->count(),
            ]);

            return $rows;
        } catch (LimitReachedException $e) {
            throw $e;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
