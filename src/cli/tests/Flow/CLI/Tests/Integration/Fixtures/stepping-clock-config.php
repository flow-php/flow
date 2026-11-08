<?php

declare(strict_types=1);

use Flow\ETL\Tests\Double\SteppingClock;

use function Flow\ETL\DSL\config_builder;

return config_builder()->clock(new SteppingClock());
