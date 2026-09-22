<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Context\FloeEngineContext;

use function Flow\Filesystem\DSL\path;

/**
 * The fixture is a 0x02 file written before the footer carried a statistics block - byte for byte, it cannot be
 * regenerated once the writer changed.
 */
final class FloeHardBreakTest extends FlowIntegrationTestCase
{
    public function test_reading_a_file_written_before_the_statistics_block_fails_and_names_the_rewrite(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessageMatches('/Floe footer is malformed: .*statistics/');

        FloeEngineContext::phpReader($this->fs())
            ->read(path(__DIR__ . '/../Fixtures/pre-statistics/heterogeneous.floe'))
            ->schema();
    }
}
