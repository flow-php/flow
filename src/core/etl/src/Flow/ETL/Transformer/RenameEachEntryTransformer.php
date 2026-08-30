<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\RowProjection;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Rename\RenameEntryStrategy;
use Throwable;

use function array_search;
use function is_string;

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
            $schema = $rows->schema();
            $renames = [];

            foreach ($this->strategies as $strategy) {
                foreach ($strategy->renames($schema) as $from => $to) {
                    $schema = $schema->rename($from, $to);

                    // strategies chain, so a later one renames what an earlier one produced; the map
                    // has to stay keyed by the row's original name or the projection lands short
                    $original = array_search($from, $renames, true);

                    $renames[is_string($original) ? $original : $from] = $to;
                }
            }

            $projection = new RowProjection();

            $result = $rows->map($schema, static fn(Row $row): Row => $projection->rename($row, $renames));

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
