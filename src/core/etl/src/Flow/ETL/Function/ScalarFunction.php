<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

interface ScalarFunction extends FunctionTree
{
    public function eval(Row $row, FlowContext $context): mixed;

    /**
     * The type of the column this function produces, before any row is read.
     *
     * Column-representable only: MixedType and a multi-member UnionType are refused; NullType is
     * permitted. A top-level OptionalType is permitted and is what "this column is nullable"
     * means - it is the only legal OptionalType position for a declaration, because OptionalType
     * never nests and type_bare() strips exactly one level.
     *
     * @return Type<mixed>
     */
    public function returns(): Type;
}
