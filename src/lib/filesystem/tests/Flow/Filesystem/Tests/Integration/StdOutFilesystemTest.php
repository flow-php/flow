<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use function Flow\Filesystem\DSL\path_stdout;
use Flow\Filesystem\Local\StdOut\Filter\Intercept;
use Flow\Filesystem\Local\StdOutFilesystem;
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class StdOutFilesystemTest extends TestCase
{
    public function test_get_system_tmp_dir() : void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('StdOut does not have a system tmp directory');

        $filesystem->getSystemTmpDir();
    }

    public function test_it_can_append_to_stdout() : void
    {
        $filesystem = new StdOutFilesystem($filter = new Intercept());
        $filter::$buffer = '';

        $destination = $filesystem->appendTo(new Path('stdout://'));

        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        self::assertSame('Hello World!', $filter::$buffer);
        $filter::$buffer = '';
    }

    public function test_it_can_write_to_output() : void
    {
        $filesystem = new StdOutFilesystem();

        $destination = $filesystem->writeTo(new Path('stdout://', ['stream' => 'output']));

        ob_start();
        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        $output = ob_get_clean();

        self::assertSame('Hello World!', $output);
    }

    public function test_it_can_write_to_stdout() : void
    {
        $filesystem = new StdOutFilesystem($filter = new Intercept());
        $filter::$buffer = '';

        $destination = $filesystem->writeTo(new Path('stdout://'));

        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        self::assertSame('Hello World!', $filter::$buffer);
        $filter::$buffer = '';
    }

    public function test_it_cant_write_to_memory() : void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Invalid output stream, allowed values are "stdout", "stderr" and "output", given: memory');

        $destination = $filesystem->writeTo(new Path('stdout://', ['stream' => 'memory']));
    }

    public function test_list() : void
    {
        $filesystem = new StdOutFilesystem();

        $paths = iterator_to_array($filesystem->list(path_stdout()));

        self::assertCount(0, $paths);
    }

    public function test_mv() : void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot move files around in stdout');

        $filesystem->mv(path_stdout(), path_stdout());
    }

    public function test_protocol() : void
    {
        $filesystem = new StdOutFilesystem();

        self::assertSame('stdout://', $filesystem->protocol()->scheme());
    }

    public function test_read_from() : void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot read from stdout');

        $filesystem->readFrom(path_stdout());
    }

    public function test_rm() : void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot read from stdout');

        $filesystem->rm(path_stdout());
    }

    public function test_status() : void
    {
        $filesystem = new StdOutFilesystem();

        self::assertNull($filesystem->status(path_stdout()));
    }
}
