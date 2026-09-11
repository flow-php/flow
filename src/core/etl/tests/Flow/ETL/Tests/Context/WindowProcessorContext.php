<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;

use function Flow\ETL\DSL\flow_context;

final class WindowProcessorContext
{
    /**
     * Rows the processor produced, as arrays.
     *
     * @param Definition<mixed>|string $entry
     *
     * @return list<Rows>
     */
    public static function batches(string|Definition $entry, WindowFunction $function, Rows $rows): array
    {
        $input = (static function () use ($rows) {
            yield $rows;
        })();

        $batches = [];

        foreach ((new WindowProcessor($entry, $function))->process($input, flow_context()) as $batch) {
            $batches[] = $batch;
        }

        return $batches;
    }

    /**
     * Values the processor wrote into $entry, in partition-processing order.
     *
     * @param Definition<mixed>|string $entry
     *
     * @return list<mixed>
     */
    public static function values(string|Definition $entry, WindowFunction $function, Rows $rows): array
    {
        $name = $entry instanceof Definition ? $entry->entry()->name() : $entry;
        $values = [];

        foreach (self::batches($entry, $function, $rows) as $batch) {
            foreach ($batch as $row) {
                $values[] = $row->get($name);
            }
        }

        return $values;
    }
}
