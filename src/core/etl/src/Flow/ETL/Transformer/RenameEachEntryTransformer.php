<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Rename\RenameEntryStrategy;
use Throwable;

final readonly class RenameEachEntryTransformer implements Transformer
{
    /**
     * @var array<RenameEntryStrategy>
     */
    private array $strategies;

    public function __construct(RenameEntryStrategy ...$strategies)
    {
        if ([] === $strategies) {
            throw new InvalidArgumentException('At least one strategy must be provided.');
        }

        $this->strategies = $strategies;
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->map(function (Row $row): Row {
                foreach ($this->strategies as $strategy) {
                    $row = $strategy->rename($row);
                }

                return $row;
            });

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
