<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row, Rows, Window};

interface WindowFunction
{
    public function apply(Row $row, Rows $partition, FlowContext $context) : mixed;

    public function over(Window $window) : self;

    public function toString() : string;

    public function window() : Window;
}
