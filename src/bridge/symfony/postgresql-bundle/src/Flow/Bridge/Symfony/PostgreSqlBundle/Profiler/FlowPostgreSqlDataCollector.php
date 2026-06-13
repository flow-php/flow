<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use Flow\PostgreSql\Client\Debug\QueryLog;
use Flow\PostgreSql\Client\Debug\RecordedQuery;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\DataCollector\DataCollector;
use Symfony\Component\HttpKernel\DataCollector\LateDataCollectorInterface;
use Throwable;

use function array_keys;
use function count;
use function in_array;
use function is_float;
use function ltrim;
use function preg_split;
use function strtoupper;

/**
 * @phpstan-type QueryRow array{statement: string, parameters: array<int, mixed>, returnedRows: null|int, durationMs: float, failed: bool, error: null|string, connection: string, caller: null|string, explainable: bool, runCount: int, isDuplicate: bool}
 */
final class FlowPostgreSqlDataCollector extends DataCollector implements LateDataCollectorInterface
{
    private const array EXPLAINABLE_KEYWORDS = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'VALUES', 'TABLE'];

    public function __construct(
        private readonly QueryLog $queryLog,
        private readonly bool $includeParameters,
    ) {}

    public function collect(Request $request, Response $response, ?Throwable $exception = null): void {}

    public function lateCollect(): void
    {
        $queries = $this->queryLog->queries();

        $runCounts = [];

        foreach ($queries as $query) {
            $runCounts[$query->sql] = ($runCounts[$query->sql] ?? 0) + 1;
        }

        $byConnection = [];
        $failedCount = 0;
        $totalDurationMs = 0.0;

        foreach ($queries as $query) {
            $totalDurationMs += $query->durationMs;

            if ($query->failed) {
                $failedCount++;
            }

            $byConnection[$query->connection][] = [
                'statement' => $query->sql,
                'parameters' => $this->includeParameters ? $query->parameters : [],
                'returnedRows' => $query->rowCount,
                'durationMs' => $query->durationMs,
                'failed' => $query->failed,
                'error' => $query->error,
                'connection' => $query->connection,
                'caller' => $query->caller,
                'explainable' => $this->isExplainable($query),
                'runCount' => $runCounts[$query->sql],
                'isDuplicate' => $runCounts[$query->sql] > 1,
            ];
        }

        $this->data = [
            'queries' => $byConnection,
            'queryCount' => count($queries),
            'failedCount' => $failedCount,
            'totalDurationMs' => $totalDurationMs,
            'duplicateCount' => count($queries) - count($runCounts),
        ];
    }

    public function reset(): void
    {
        $this->data = [];
        $this->queryLog->reset();
    }

    public function getName(): string
    {
        return 'flow_postgresql';
    }

    /**
     * @return array<string, list<QueryRow>>
     */
    public function getQueries(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['queries'] ?? [];
    }

    /**
     * @return list<string>
     */
    public function getConnections(): array
    {
        return array_keys($this->getQueries());
    }

    public function getQueryCount(): int
    {
        return (int) ($this->data['queryCount'] ?? 0);
    }

    public function getFailedCount(): int
    {
        return (int) ($this->data['failedCount'] ?? 0);
    }

    public function getDuplicateCount(): int
    {
        return (int) ($this->data['duplicateCount'] ?? 0);
    }

    public function getTotalDurationMs(): float
    {
        // @mago-expect analysis:mixed-assignment
        $value = $this->data['totalDurationMs'] ?? 0.0;

        return is_float($value) ? $value : 0.0;
    }

    private function isExplainable(RecordedQuery $query): bool
    {
        if ($query->failed) {
            return false;
        }

        $words = preg_split('/\s+/', ltrim($query->sql), 2);

        if ($words === false || $words === []) {
            return false;
        }

        return in_array(strtoupper($words[0]), self::EXPLAINABLE_KEYWORDS, true);
    }
}
