<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class ScalarFunctions implements ExpandResults, ScalarFunction, UnpackResults
{
    use EntryScalarFunction;

    /**
     * @var array<ScalarFunction>
     */
    private array $functions;

    public function __construct(ScalarFunction ...$functions)
    {
        $this->functions = $functions;
    }

    public function eval(Row $row) : mixed
    {
        $lastValue = null;

        foreach ($this->functions as $function) {
            $lastValue = $function->eval($row);

            if ($function instanceof Reference) {
                $row = $row->set((new Row\Factory\NativeEntryFactory())->create($function->to(), $lastValue));
            }
        }

        return $lastValue;
    }

    public function expand() : bool
    {
        foreach ($this->functions as $function) {
            if ($function instanceof ExpandResults) {
                return $function->expand();
            }
        }

        return false;
    }

    public function unpack() : bool
    {
        foreach ($this->functions as $function) {
            if ($function instanceof UnpackResults) {
                return $function->unpack();
            }
        }

        return false;
    }
}
