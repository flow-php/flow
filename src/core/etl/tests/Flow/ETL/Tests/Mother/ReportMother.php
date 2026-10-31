<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DateTimeImmutable;
use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\Runtime\Metrics\Processes;
use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Runtime\Metrics\Statistics;
use Flow\ETL\Runtime\Metrics\Statistics\Columns;
use Flow\ETL\Runtime\Metrics\Statistics\ExecutionTime;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Consumption;
use Flow\ETL\Values\Schema;

final class ReportMother
{
    /**
     * A run of $totalRows rows that took one second, from 2025-01-01 00:00:00 UTC.
     *
     * @param null|list<SourceStatistics> $sources
     */
    public static function of(
        Processes $processes,
        int $totalRows = 0,
        ?Schema $schema = null,
        ?array $sources = null,
        ?Columns $columns = null,
    ): Report {
        return new Report(
            $schema,
            new Statistics(
                $totalRows,
                new ExecutionTime(
                    new DateTimeImmutable('2025-01-01 00:00:00 UTC'),
                    new DateTimeImmutable('2025-01-01 00:00:01 UTC'),
                    new HighResolutionTime(1, 0),
                ),
                new Consumption(new PhpBackend()),
                $columns,
                $processes,
            ),
            $sources,
        );
    }
}
