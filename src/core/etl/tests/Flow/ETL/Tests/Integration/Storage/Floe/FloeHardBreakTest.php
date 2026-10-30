<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Storage\Floe;

use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\Frame\FloeException;

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

        FloeFilesContext::files($this->fs())
            ->read(path(__DIR__ . '/../../../Fixtures/Floe/pre-statistics/heterogeneous.floe'))
            ->schema();
    }
}
