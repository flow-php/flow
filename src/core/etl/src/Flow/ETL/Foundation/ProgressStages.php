<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

interface ProgressStages
{
    public const string COMMIT = 'commit';

    public const string GROUP_BY = 'group_by';

    public const string INFER = 'infer';

    public const string JOIN = 'join';

    public const string PARALLEL = 'parallel';

    public const string PIVOT = 'pivot';

    public const string READ = 'read';

    public const string REPARTITION = 'repartition';

    public const string SORT = 'sort';

    public const string WINDOW = 'window';

    public function stage(
        string $name,
        ProgressUnit $unit = ProgressUnit::rows,
        ?int $id = null,
        ?string $location = null,
    ): Progress;
}
