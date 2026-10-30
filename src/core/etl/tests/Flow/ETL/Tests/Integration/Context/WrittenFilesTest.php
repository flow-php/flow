<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Context;

use Flow\ETL\Tests\Context\WrittenFiles;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function file_put_contents;
use function mkdir;

final class WrittenFilesTest extends FlowIntegrationTestCase
{
    public function test_temporary_outputs_finds_a_planted_directory_at_any_depth(): void
    {
        mkdir($this->cacheDir->path() . '/out/year=2024/.flow-tmp-x/00000', 0777, true);
        mkdir($this->cacheDir->path() . '/.flow-tmp-y');
        file_put_contents($this->cacheDir->path() . '/out/part.csv', 'id');

        static::assertSame(
            ['.flow-tmp-y', 'out/year=2024/.flow-tmp-x'],
            WrittenFiles::temporaryOutputs($this->cacheDir),
        );
    }

    public function test_under_lists_every_file_relative_and_sorted(): void
    {
        mkdir($this->cacheDir->path() . '/out/year=2024', 0777, true);
        file_put_contents($this->cacheDir->path() . '/out/year=2024/b.csv', 'id');
        file_put_contents($this->cacheDir->path() . '/out/a.csv', 'id');

        static::assertSame(['a.csv', 'year=2024/b.csv'], WrittenFiles::under($this->cacheDir->suffix('/out')));
    }
}
