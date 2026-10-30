<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\IO\Loader;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class SerialRunTest extends FlowIntegrationTestCase
{
    public function test_limit(): void
    {
        $path = $this->cacheDir->path() . '/limit.floe';

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->write(to_floe($path))
            ->run();

        static::assertSame(3, df()->read(from_floe($path))->limit(3)->count());
    }

    public function test_not_calling_loader_when_rows_are_empty(): void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects(self::never())->method('load');

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ]))
            ->filter(lit(true)->equals(false))
            ->write($loader)
            ->run();
    }
}
