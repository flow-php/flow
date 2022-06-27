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
        $resource = Handler::directory('json')
            ->open(new LocalFile(\sys_get_temp_dir() . '/nested/directory'), Mode::WRITE);

        $this->assertIsResource($resource);

        @\unlink($this->getFileNameFromResource($resource));
    }

    public function test_directory_handler_without_double_dot() : void
    {
        $resource = Handler::directory('.json')
            ->open(new LocalFile(\sys_get_temp_dir() . '/nested/directory'), Mode::WRITE);

        $fileName = $this->getFileNameFromResource($resource);

        $this->assertStringNotContainsString('..json', $fileName);

        @\unlink($fileName);
    }

    public function test_file_handler() : void
    {
        $resource = Handler::file()
            ->open(new LocalFile(\sys_get_temp_dir() . '/file.json'), Mode::WRITE);

        $this->assertIsResource($resource);

        @\unlink($this->getFileNameFromResource($resource));
    }

    /**
     * @param resource $resource
     */
    private function getFileNameFromResource($resource) : string
    {
        return \stream_get_meta_data($resource)['uri'];
    }
}
