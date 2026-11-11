<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Runtime;

use Closure;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\Double\SteppingClock;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Memory\Unit;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_filter;
use function array_map;
use function array_merge;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function max;
use function range;

final class HeartbeatTest extends FlowIntegrationTestCase
{
    private const array ROWS = [
        ['id' => 3, 'group' => 'a', 'name' => 'x', 'amount' => 10],
        ['id' => 1, 'group' => 'b', 'name' => 'y', 'amount' => 20],
        ['id' => 2, 'group' => 'a', 'name' => 'y', 'amount' => 30],
        ['id' => 4, 'group' => 'b', 'name' => 'x', 'amount' => 40],
    ];

    /**
     * @return Generator<string, array{Closure(DataFrame): DataFrame, string}>
     */
    public static function blocking_steps(): Generator
    {
        yield 'sort' => [static fn(DataFrame $df): DataFrame => $df->sortBy(ref('id')), 'sort'];
        yield 'group by' => [
            static fn(DataFrame $df): DataFrame => $df->groupBy(ref('group'))->aggregate(sum(ref('amount'))),
            'group_by',
        ];
        yield 'pivot' => [
            static fn(DataFrame $df): DataFrame => $df
                ->groupBy(ref('group'))
                ->pivot(ref('name'), pivot_values('x', 'y'))
                ->aggregate(sum(ref('amount'))),
            'pivot',
        ];
        yield 'join' => [
            static fn(DataFrame $df): DataFrame => $df->join(
                df()->read(from_array([['id' => 1], ['id' => 2]])),
                join_on(['id' => 'id'], 'r_'),
            ),
            'join',
        ];
        yield 'window' => [
            static fn(DataFrame $df): DataFrame => $df->withColumn(
                'rn',
                row_number()->over(window()->partitionBy(ref('group'))->orderBy(ref('id'))),
            ),
            'window',
        ];
        yield 'window without a partition' => [
            static fn(DataFrame $df): DataFrame => $df->withColumn(
                'rn',
                row_number()->over(window()->orderBy(ref('id'))),
            ),
            'window',
        ];
    }

    /**
     * @param Closure(DataFrame): DataFrame $step
     */
    #[DataProvider('blocking_steps')]
    public function test_a_blocking_step_beats_with_its_stage(Closure $step, string $stage): void
    {
        $recording = new RecordingHeartbeat();

        $step(
            df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
                ->read(from_array(self::ROWS)),
        )->run();

        static::assertNotSame(
            [],
            array_filter(
                HeartbeatContext::entries($recording, StageName::from($stage)),
                static fn(array $entries): bool => $entries !== [],
            ),
        );
    }

    public function test_a_spilling_sort_counts_every_row_once(): void
    {
        $recording = new RecordingHeartbeat();
        $rows = array_map(static fn(int $id): array => ['id' => $id], range(20, 1));

        df(
            config_builder()
                ->clock(new SteppingClock())
                ->sort(external_sort()->memoryLimit(Unit::fromBytes(1))->bucketsCount(2))
                ->heartbeat($recording, everySeconds: 1),
        )
            ->read(from_array($rows))
            ->batchSize(1)
            ->sortBy(ref('id'))
            ->run();

        static::assertSame(
            20,
            max(array_map(
                static fn(StageProgress $sort): int => $sort->count,
                array_merge(...HeartbeatContext::entries($recording, StageName::sort)),
            )),
        );
    }
}
