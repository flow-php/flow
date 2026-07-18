<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;

final readonly class SingleBucketJoinKeys implements JoinKeys
{
    public function leftHash(Row $row): string
    {
        return '';
    }

    public function rightHash(Row $row): string
    {
        return '';
    }
}
