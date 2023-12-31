<?php declare(strict_types=1);

namespace Flow\ETL\Function;

interface CompositeScalarFunction extends ScalarFunction
{
    /**
     * @return array<ScalarFunction>
     */
    public function functions() : array;
}
