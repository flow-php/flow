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

        df(config_builder()->clock(new SteppingClock())->heartbeat(console_heartbeat($output), everySeconds: 1))
            ->read(from_array([['id' => 5], ['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->sortBy(ref('id'))
            ->run();

        $printed = $output->fetch();

        static::assertMatchesRegularExpression('/^ read: /m', $printed);
        static::assertMatchesRegularExpression('/^ sort: /m', $printed);
    }
}
