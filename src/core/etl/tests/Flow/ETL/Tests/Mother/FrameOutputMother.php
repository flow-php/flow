<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Execution\Run;
use Flow\ETL\Extractor;
use Flow\ETL\Plan\FrameOutput;
use Flow\ETL\Planner;

use function Flow\ETL\DSL\config;

final class FrameOutputMother
{
    public static function reading(Extractor $extractor): FrameOutput
    {
        return new FrameOutput(
            Planner::default()->plan(NodeMother::plan(NodeMother::read($extractor)), NodeMother::context()),
            Run::in(config()),
        );
    }
}
