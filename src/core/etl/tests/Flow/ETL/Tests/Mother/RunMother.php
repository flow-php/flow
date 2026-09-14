<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Execution\Run;
use Flow\ETL\Executor;
use Flow\ETL\Planner;

final class RunMother
{
    public static function default(): Run
    {
        return new Run(Planner::default(), new Executor());
    }
}
