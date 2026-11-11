<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\Bridge\Symfony\Console\ThroughputGraph;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Tests\FlowTestCase;

use function range;

final class ThroughputGraphTest extends FlowTestCase
{
    public function test_a_planned_stage_draws_no_bar_and_counts_from_its_first_start(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::planned()]));
        $planned = $graph->sparkline(StageProgressMother::planned(), 0);
        $graph->recorded(PulseMother::pulse(beat: 2, elapsedSeconds: 4.0, stages: [
            StageProgressMother::open(count: 30, startedAtSeconds: 3.0, id: 2),
        ]));

        static::assertSame(['', '<fg=cyan>█</>'], [
            $planned,
            $graph->sparkline(StageProgressMother::open(count: 30, startedAtSeconds: 3.0, id: 2), 0),
        ]);
    }

    public function test_rates_between_beats_draw_bars(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            count: 10,
        )]));
        $graph->recorded(PulseMother::pulse(beat: 2, elapsedSeconds: 2.0, stages: [StageProgressMother::open(
            count: 80,
        )]));

        static::assertSame('<fg=cyan>▂█</>', $graph->sparkline(StageProgressMother::open(count: 80), 0));
    }

    public function test_keeps_the_last_twelve_rates(): void
    {
        $graph = new ThroughputGraph();

        foreach (range(1, 15) as $beat) {
            $graph->recorded(PulseMother::pulse(
                beat: $beat,
                elapsedSeconds: (float) $beat,
                stages: [StageProgressMother::open(count: $beat * 10)],
            ));
        }

        static::assertSame('<fg=cyan>████████████</>', $graph->sparkline(StageProgressMother::open(), 0));
    }

    public function test_two_entries_of_one_name_keep_two_histories(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [
            StageProgressMother::open(count: 100, id: 1),
            StageProgressMother::open(count: 0, id: 2),
        ]));

        static::assertSame(['<fg=cyan>█</>', '<fg=cyan>▁</>'], [
            $graph->sparkline(StageProgressMother::open(id: 1), 0),
            $graph->sparkline(StageProgressMother::open(id: 2), 0),
        ]);
    }

    public function test_an_ended_entry_is_gray_and_an_unseen_one_has_no_graph(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            count: 100,
        )]));

        static::assertSame(['<fg=gray>█</>', ''], [
            $graph->sparkline(StageProgressMother::done(), 0),
            $graph->sparkline(StageProgressMother::open(id: 9), 0),
        ]);
    }

    public function test_a_first_beat_starts_new_histories(): void
    {
        $graph = new ThroughputGraph();
        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            count: 100,
        )]));

        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            count: 0,
        )]));

        static::assertSame('<fg=cyan>▁</>', $graph->sparkline(StageProgressMother::open(), 0));
    }

    public function test_a_count_in_another_unit_starts_a_new_history(): void
    {
        $graph = new ThroughputGraph();
        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            StageName::group_by,
            1_000,
        )]));

        $graph->recorded(PulseMother::pulse(beat: 2, elapsedSeconds: 2.0, stages: [
            StageProgressMother::open(StageName::group_by, 3, unit: ProgressUnit::groups),
        ]));

        static::assertSame('<fg=cyan>█</>', $graph->sparkline(
            StageProgressMother::open(StageName::group_by, 3, unit: ProgressUnit::groups),
            0,
        ));
    }

    public function test_an_ended_entry_gets_no_new_rate(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::done(
            endedAtSeconds: 2.0,
        )]));

        static::assertSame('', $graph->sparkline(StageProgressMother::done(endedAtSeconds: 2.0), 0));
    }

    public function test_a_beat_at_the_start_of_an_entry_records_a_zero_rate(): void
    {
        $graph = new ThroughputGraph();

        $graph->recorded(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::open(
            count: 10,
            startedAtSeconds: 2.0,
        )]));

        static::assertSame('<fg=cyan>▁</>', $graph->sparkline(StageProgressMother::open(startedAtSeconds: 2.0), 0));
    }

    public function test_a_count_that_goes_down_draws_the_lowest_bar(): void
    {
        $graph = new ThroughputGraph();
        $graph->recorded(PulseMother::pulse(beat: 1, elapsedSeconds: 1.0, stages: [StageProgressMother::open(
            count: 100,
        )]));

        $graph->recorded(PulseMother::pulse(beat: 2, elapsedSeconds: 2.0, stages: [StageProgressMother::open(
            count: 50,
        )]));

        static::assertSame('<fg=cyan>█▁</>', $graph->sparkline(StageProgressMother::open(), 0));
    }
}
