<?php declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Loader;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

/**
 * @method DataFrame write(Loader $loader)
 */
final class GroupedDataFrame
{
    public function __construct(private readonly DataFrame $df)
    {
    }

    public function __call(string $name, array $arguments) : DataFrame|Rows|self|null
    {
        if (\strtolower($name) === 'pivot') {
            return $this->pivot(...$arguments);
        }

        if (\strtolower($name) === 'aggregate') {
            return $this->aggregate(...$arguments);
        }

        return $this->df->{$name}(...$arguments);
    }

    public function aggregate(AggregatingFunction ...$aggregations) : self
    {
        $this->df->aggregate(...$aggregations);

        return $this;
    }

    public function pivot(Reference $ref) : DataFrame
    {
        return $this->df->pivot($ref);
    }

    public function rename(string $from, string $to) : DataFrame
    {
        return $this->df->rename($from, $to);
    }
}
