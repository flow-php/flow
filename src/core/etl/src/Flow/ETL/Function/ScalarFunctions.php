<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;

final class ScalarFunctions implements ExpandResults, ScalarFunction, UnpackResults
{
    use EntryScalarFunction;

    /**
     * @var array<ScalarFunction>
     */
    private array $expressions;

    public function __construct(ScalarFunction ...$expressions)
    {
        $this->expressions = $expressions;
    }

    public function eval(Row $row) : mixed
    {
        $lastValue = null;

        foreach ($this->expressions as $expression) {
            $lastValue = $expression->eval($row);

            if ($expression instanceof Reference) {
                $row = $row->set((new Row\Factory\NativeEntryFactory())->create($expression->to(), $lastValue));
            }
        }

        return $lastValue;
    }

    public function expand() : bool
    {
        foreach ($this->expressions as $expression) {
            if ($expression instanceof ExpandResults) {
                return $expression->expand();
            }
        }

        return false;
    }

    public function unpack() : bool
    {
        foreach ($this->expressions as $expression) {
            if ($expression instanceof UnpackResults) {
                return $expression->unpack();
            }
        }

        return false;
    }
}
