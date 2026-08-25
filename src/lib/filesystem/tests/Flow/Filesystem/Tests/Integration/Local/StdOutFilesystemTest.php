<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\Local;

use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Local\StdOutFilesystem;
use Flow\Filesystem\Local\StreamFilter\Intercept;
use Flow\Filesystem\Mount;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;

final class StdOutFilesystemTest extends TestCase
{
    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        (new StdOutFilesystem())->appendTo(path('file:///var/foo.txt'));
    }

    public function test_append_to_same_target_as_open_write_throws(): void
    {
        $filesystem = new StdOutFilesystem();
        $filesystem->writeTo(path('stdout://a', ['stream' => 'stderr']));

        $this->expectExceptionMessage('Only one stream can be open at the same time for php://stderr');

        $filesystem->appendTo(path('stdout://b', ['stream' => 'stderr']));
    }

    public function test_close_releases_target_slot(): void
    {
        $filesystem = new StdOutFilesystem();

        $first = $filesystem->writeTo(path('stdout://a', ['stream' => 'output']));
        $first->close();

        $second = $filesystem->writeTo(path('stdout://b', ['stream' => 'output']));
        static::assertTrue($second->isOpen());
        $second->close();
    }

    public function test_get_system_tmp_dir(): void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('StdOut does not have a system tmp directory');

        $filesystem->getSystemTmpDir();
    }

    public function test_it_can_append_to_stdout(): void
    {
        $filesystem = new StdOutFilesystem(new Mount('stdout'), $filter = new Intercept());
        $filter::$buffer = '';

        $destination = $filesystem->appendTo(path('stdout://'));

        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        static::assertSame('Hello World!', $filter::$buffer);
        $filter::$buffer = '';
    }

    public function test_it_can_write_to_output(): void
    {
        $filesystem = new StdOutFilesystem();

        $destination = $filesystem->writeTo(path('stdout://', ['stream' => 'output']));

        ob_start();
        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        $output = ob_get_clean();

        static::assertSame('Hello World!', $output);
    }

    public function test_it_can_write_to_stdout(): void
    {
        $filesystem = new StdOutFilesystem(new Mount('stdout'), $filter = new Intercept());
        $filter::$buffer = '';

        $destination = $filesystem->writeTo(path('stdout://'));

        $destination->append('Hello');
        $destination->append(' ');
        $destination->append('World!');

        static::assertSame('Hello World!', $filter::$buffer);
        $filter::$buffer = '';
    }

    public function test_it_cant_write_to_memory(): void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage(
            'Invalid output stream, allowed values are "stdout", "stderr" and "output", given: memory',
        );

        $filesystem->writeTo(path('stdout://', ['stream' => 'memory']));
    }

    public function test_list(): void
    {
        $filesystem = new StdOutFilesystem();

        $paths = iterator_to_array($filesystem->list(path('stdout://x.stdout')));

        static::assertCount(0, $paths);
    }

    public function test_mount(): void
    {
        $filesystem = new StdOutFilesystem();

        static::assertSame('stdout', $filesystem->mount()->protocol);
    }

    public function test_mv(): void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot move files around in stdout');

        $filesystem->mv(path('stdout://a.stdout'), path('stdout://b.stdout'));
    }

    public function test_read_from(): void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot read from stdout');

        $filesystem->readFrom(path('stdout://x.stdout'));
    }

    public function test_rm(): void
    {
        $filesystem = new StdOutFilesystem();

        $this->expectExceptionMessage('Cannot remove files from stdout');

        $filesystem->rm(path('stdout://x.stdout'));
    }

    public function test_status(): void
    {
        $filesystem = new StdOutFilesystem();

        static::assertNull($filesystem->status(path('stdout://x.stdout')));
    }

    public function test_stdout_filesystem_supports_only_stdout_paths(): void
    {
        static::assertTrue((new StdOutFilesystem())->supports(path('stdout://output.csv')));
        static::assertFalse((new StdOutFilesystem())->supports(path('/tmp/output.csv')));
    }

    public function test_write_to_different_targets_concurrently_succeeds(): void
    {
        $filesystem = new StdOutFilesystem();

        $stdoutStream = $filesystem->writeTo(path('stdout://a', ['stream' => 'stdout']));
        $outputStream = $filesystem->writeTo(path('stdout://b', ['stream' => 'output']));

        static::assertTrue($stdoutStream->isOpen());
        static::assertTrue($outputStream->isOpen());

        $stdoutStream->close();
        $outputStream->close();
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "file://" is not supported by this protocol. Expected scheme is "stdout://"',
        );

        (new StdOutFilesystem())->writeTo(path('file:///var/foo.txt'));
    }

    public function test_write_to_same_target_twice_throws(): void
    {
        $filesystem = new StdOutFilesystem();
        $filesystem->writeTo(path('stdout://a', ['stream' => 'output']));

        $this->expectExceptionMessage('Only one stream can be open at the same time for php://output');

        $filesystem->writeTo(path('stdout://b', ['stream' => 'output']));
    }
}
