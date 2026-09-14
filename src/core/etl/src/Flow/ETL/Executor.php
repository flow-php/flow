<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Extractor\Scannable;
use Generator;
use Throwable;

use function array_reverse;
use function in_array;
use function sprintf;

final readonly class Executor
{
    /**
     * @return Generator<int, Rows>
     */
    public function execute(Plan\Pipeline $pipeline): Generator
    {
        $chain = [];

        for ($stage = $pipeline; $stage !== null; $stage = $stage->input()) {
            $chain[] = $stage;
        }

        $chain = array_reverse($chain);
        $leaf = $chain[0];
        $source = $leaf->segments()->extractor() ?? throw InvalidLogicException::pipelineWithoutSource(sprintf(
            'pipeline #%d',
            $leaf->id,
        ));
        $generator = $source instanceof Scannable
            ? $source->extract($leaf->context(), $leaf->scan())
            : $source->extract($leaf->context());

        $embedded = [];

        foreach ($chain as $stage) {
            $context = $stage->context();

            if ($context !== $pipeline->context() && !in_array($context, $embedded, true)) {
                $embedded[] = $context;
            }

            foreach ($stage->segments()->all() as $segment) {
                $generator = $segment->execute($generator, $context);
                $processor = $segment->processor();

                if ($processor !== null) {
                    $generator = $processor->process($generator, $context);
                }
            }
        }

        foreach ($embedded as $context) {
            $context->telemetry()->dataFrameStarted($context);
        }

        try {
            // a foreach, never `yield from`: the re-yield must not forward the consumer's sent signal
            foreach ($generator as $rows) {
                yield $rows;
            }
        } catch (Throwable $e) {
            foreach ($embedded as $context) {
                $context->telemetry()->dataFrameFailed($context, $e);
            }

            throw $e;
        } finally {
            // drained, abandoned, or destroyed because the consumer's body threw - the same shape as
            // Execution\Run::execute(), and a no-op once dataFrameFailed() closed the span
            foreach ($embedded as $context) {
                $context->telemetry()->dataFrameCompleted($context);
            }
        }
    }
}
