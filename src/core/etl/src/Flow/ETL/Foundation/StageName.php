<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

// backed, so a description and a worker report carry the text and read it back with StageName::from()
enum StageName: string
{
    case read = 'read';
    case infer = 'infer';
    case sort = 'sort';
    case group_by = 'group_by';
    case pivot = 'pivot';
    case join = 'join';
    case window = 'window';
    case repartition = 'repartition';
    case write = 'write';
    case commit = 'commit';
    case parallel = 'parallel';
}
