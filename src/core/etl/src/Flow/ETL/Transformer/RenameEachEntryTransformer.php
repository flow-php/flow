<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
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

    public function bind(Schema $input): BoundStep
    {
        $output = $input;

        foreach ($this->strategies as $strategy) {
            foreach ($strategy->renames($output) as $from => $to) {
                $output = $output->rename((string) $from, $to);
            }
        }

        return new BoundStep($this, $output);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $schema = $rows->schema();
            // strategies chain, so a later one renames what an earlier one produced
            foreach ($this->strategies as $strategy) {
                foreach ($strategy->renames($schema) as $from => $to) {
                    // a numeric entry name arrives as int - PHP casts numeric-string array keys
                    $schema = $schema->rename((string) $from, $to);
                }
            }

            $result = $rows->withSchema($schema);

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
