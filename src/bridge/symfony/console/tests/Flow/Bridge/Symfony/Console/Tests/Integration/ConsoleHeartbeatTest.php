<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Integration;

use Flow\ETL\Tests\Double\SteppingClock;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Symfony\Component\Console\Output\BufferedOutput;

use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;

final class ConsoleHeartbeatTest extends FlowIntegrationTestCase
{
    public function test_a_run_prints_its_stages(): void
    {
        $output = new BufferedOutput();
        $heartbeat = console_heartbeat($output);

        try {
            df(config_builder()->clock(new SteppingClock())->heartbeat($heartbeat, everySeconds: 1))
                ->read(from_array([['id' => 5], ['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
                ->sortBy(ref('id'))
                ->run();
        } finally {
            $heartbeat->finish();
        }

        $printed = $output->fetch();

        static::assertMatchesRegularExpression('/^\[00:\d\d\] read      done 5 rows/m', $printed);
        static::assertMatchesRegularExpression('/^\[00:\d\d\] sort      done /m', $printed);
        static::assertMatchesRegularExpression('/^\[00:\d\d\] finished  5 rows in /m', $printed);
    }
}
