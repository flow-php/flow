<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Stream;

use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\LocalFile;
use Flow\ETL\Stream\Mode;
use PHPUnit\Framework\TestCase;

final class HandlerTest extends TestCase
{
    public function test_directory_handler() : void
    {
        $handler = Handler::directory('json');

        $resource = $handler->open(new LocalFile(\sys_get_temp_dir() . '/nested/directory'), Mode::WRITE);

        $this->assertIsResource($resource);

        \fclose($resource);
    }

    public function test_file_handler() : void
    {
        $handler = Handler::file();

        $resource = $handler->open(new LocalFile(\sys_get_temp_dir() . '/file.json'), Mode::WRITE);

        $this->assertIsResource($resource);

        \fclose($resource);
    }
}
