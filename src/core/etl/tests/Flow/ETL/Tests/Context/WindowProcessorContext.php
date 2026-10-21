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
     * @param Definition<mixed>|string $column
     *
     * @return list<Rows>
     */
    public static function batches(string|Definition $column, WindowFunction $function, Rows $rows): array
    {
        $input = (static function () use ($rows) {
            yield $rows;
        })();

        $batches = [];

        foreach ((new WindowProcessor($column, $function))->process($input, flow_context()) as $batch) {
            $batches[] = $batch;
        }

        return $batches;
    }

    /**
     * Values the processor wrote into $column, in partition-processing order.
     *
     * @param Definition<mixed>|string $column
     *
     * @return list<mixed>
     */
    public static function values(string|Definition $column, WindowFunction $function, Rows $rows): array
    {
        $name = $column instanceof Definition ? $column->reference()->name() : $column;
        $values = [];

        foreach (self::batches($column, $function, $rows) as $batch) {
            foreach ($batch->toArray() as $row) {
                $values[] = $row[$name];
            }
        }

        return $values;
    }
}
