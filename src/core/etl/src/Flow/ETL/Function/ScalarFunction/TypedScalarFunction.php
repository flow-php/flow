<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ScalarFunction;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\PHP\Type\Type;

interface TypedScalarFunction extends ScalarFunction
{
    /**
     * Defines the return type of the function which can be used to
     * create a type-safe entry even from non deterministic values.
     *
     * Non deterministic values satisfies more than one type, so the return type is needed to be specified.
     * For example, `[]` (empty array) can be one of:
     *
     * - array
     * - list<any>
     * - map<any,any>
     *
     * @return Type<mixed>
     */
    public function returns() : Type;
}
