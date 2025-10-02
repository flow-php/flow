<?php

declare(strict_types=1);

namespace Flow\ETL\Time;

interface Sleep
{
    public function for(Duration $duration) : void;
}
