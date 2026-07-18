<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;

interface JoinKeys
{
    public function leftHash(Row $row): string;

    public function rightHash(Row $row): string;
}
