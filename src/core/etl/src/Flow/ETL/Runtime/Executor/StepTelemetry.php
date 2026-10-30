<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Telemetry\SpanAttributes;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Telemetry\StepKind;
use Flow\ETL\Values\Rows;
use Generator;
use Throwable;

final readonly class StepTelemetry
{
    public function load(Loader $loader, Rows $rows, FlowContext $context): void
    {
        if (!$context->telemetry()->options->traceLoading) {
            $loader->load($rows, $context->backend());

            return;
        }

        $step = $context->telemetry()->stepStarted(StepKind::loading, $loader::class, [
            ...(
                $loader instanceof FileLoader
                    ? [TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $loader->destination()->uri()]
                    : []
            ),
            ...($loader instanceof SpanAttributes ? $loader->spanAttributes() : []),
        ]);

        try {
            $loader->load($rows, $context->backend());
        } catch (Throwable $e) {
            $context->telemetry()->stepFailed($step, StepKind::loading, $loader::class, $e);

            throw $e;
        }

        $context->telemetry()->stepCompleted($step, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
    }

    /**
     * One span per operator per run: wall time from the first pull of $batches to its last batch, or to an early stop.
     * With transformations untraced, $batches itself.
     *
     * @param class-string $class the processor or join yielding $batches
     * @param Generator<Rows> $batches
     *
     * @return Generator<Rows>
     */
    public function operator(string $class, Generator $batches, FlowContext $context): Generator
    {
        if (!$context->telemetry()->options->traceTransformations) {
            return $batches;
        }

        return (static function () use ($class, $batches, $context): Generator {
            $step = $context->telemetry()->stepStarted(StepKind::operator, $class);

            try {
                // send() resumes like next() and forwards the consumer's stop to the operator
                while ($batches->valid()) {
                    $batches->send(yield $batches->current());
                }

                return $batches->getReturn();
            } catch (Throwable $e) {
                $context->telemetry()->stepFailed($step, StepKind::operator, $class, $e);

                throw $e;
            } finally {
                // a no-op after a failure; after an early stop, the abandoned operator completes here
                $context->telemetry()->stepCompleted($step);
            }
        })();
    }

    public function transform(Transformer $transformer, Rows $rows, FlowContext $context): Rows
    {
        if (!$context->telemetry()->options->traceTransformations) {
            return $transformer->transform($rows, $context->backend());
        }

        $step = $context->telemetry()->stepStarted(
            StepKind::transformation,
            $transformer::class,
            $transformer instanceof SpanAttributes ? $transformer->spanAttributes() : [],
        );

        try {
            $result = $transformer->transform($rows, $context->backend());
        } catch (Throwable $e) {
            $context->telemetry()->stepFailed($step, StepKind::transformation, $transformer::class, $e);

            throw $e;
        }

        $context->telemetry()->stepCompleted($step, [
            TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
            TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
        ]);

        return $result;
    }
}
