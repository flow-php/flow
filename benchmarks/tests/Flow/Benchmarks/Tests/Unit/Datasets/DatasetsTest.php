<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Datasets;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Tests\Context\ProjectRoot;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

use function is_dir;

final class DatasetsTest extends TestCase
{
    public function test_reset_refuses_a_directory_outside_benchmarks_var(): void
    {
        $projectRoot = new ProjectRoot('reset_guard');
        $decoy = $projectRoot->linkVarOutside();
        $thrown = null;

        try {
            Datasets::reset();
        } catch (Throwable $exception) {
            $thrown = $exception;
        }

        $decoySurvived = is_dir($decoy);
        $projectRoot->release();

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertStringContainsString('Refusing to clear unexpected directory', $thrown->getMessage());
        static::assertTrue($decoySurvived);
    }
}
