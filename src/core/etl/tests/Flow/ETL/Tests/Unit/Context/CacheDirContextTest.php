<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Context;

use Flow\ETL\Tests\Context\CacheDirContext;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;

use function Flow\Filesystem\DSL\path;
use function getcwd;

final class CacheDirContextTest extends FlowTestCase
{
    #[TestWith(['/'])]
    #[TestWith([''])]
    public function test_the_filesystem_root_is_refused(string $cacheDir): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Refusing to run integration tests with cache dir resolved to "/" — would delete root.',
        );

        CacheDirContext::assertSafe(path($cacheDir));
    }

    public function test_the_current_working_directory_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Refusing to run integration tests with cache dir equal to the current working directory ("'
            . (string) getcwd()
            . '"). Set FLOW_LOCAL_FILESYSTEM_CACHE_DIR to a dedicated temp directory.',
        );

        CacheDirContext::assertSafe(path((string) getcwd()));
    }

    public function test_a_dedicated_directory_passes(): void
    {
        CacheDirContext::assertSafe(path('/flow-php-tests/cache'));

        $this->addToAssertionCount(1);
    }
}
