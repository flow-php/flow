<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Generator;

use function Flow\Types\DSL\type_instance_of;

final class ExpandingProcessorContext
{
    /**
     * @param Processor|Transformer $processor as BoundStep::$step types it
     *
     * @return list<Rows>
     */
    public static function batches(FlowContext $context, Processor|Transformer $processor, Rows ...$input): array
    {
        $generator = (static function () use ($input): Generator {
            yield from $input;
        })();

        return iterator_to_array(
            type_instance_of(Processor::class)->assert($processor)->process($generator, $context),
            false,
        );
    }

    /**
     * Every row the processor emitted, across its batches.
     *
     * @return list<array<array-key, mixed>>
     */
    public static function rows(FlowContext $context, Processor|Transformer $processor, Rows ...$input): array
    {
        $rows = [];

        foreach (self::batches($context, $processor, ...$input) as $batch) {
            foreach ($batch->toArray() as $row) {
                $rows[] = $row;
            }
        }

        return $rows;
    }
}
