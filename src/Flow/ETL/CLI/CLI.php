<?php declare(strict_types=1);

namespace Flow\ETL\CLI;

interface CLI
{
    public function run(Input $input) : int;
}
