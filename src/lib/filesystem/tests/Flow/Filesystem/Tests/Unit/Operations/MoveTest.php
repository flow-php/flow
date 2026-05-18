<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Operations;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Operations\Copy;
use Flow\Filesystem\Operations\Move;
use Flow\Filesystem\Tests\Double\FailingRmFilesystem;
use Flow\Filesystem\Tests\Double\ThrowingSourceFilesystem;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function file_get_contents;
use function file_put_contents;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function random_bytes;
use function sys_get_temp_dir;
use function unlink;

final class MoveTest extends TestCase
{
    public function test_cross_mount_move_propagates_iteration_error_and_closes_streams(): void
    {
        $memory = memory_filesystem('src');
        $src = path('src://file.txt');

        $throwing = new ThrowingSourceFilesystem($memory);
        $dest = path('dest://file.txt');

        $initStream = $memory->writeTo($src);
        $initStream->append('payload');
        $initStream->close();

        $fstab = new FilesystemTable($throwing, memory_filesystem('dest'));

        try {
            (new Copy($fstab))->execute($src, $dest);
            static::fail('Expected RuntimeException to be thrown from throwing iterate()');
        } catch (RuntimeException $e) {
            static::assertSame('Throwing source stream failed mid-iterate', $e->getMessage());
        }

        static::assertNotNull($throwing->lastStream);
        static::assertTrue($throwing->lastStream->closed, 'Source stream must be closed by the finally block');
    }

    public function test_cross_mount_move_returns_false_when_source_rm_fails(): void
    {
        $fstab = new FilesystemTable(new FailingRmFilesystem(memory_filesystem('src')), memory_filesystem('dest'));

        $src = path('src://file.txt');
        $dest = path('dest://file.txt');

        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('move-payload');
        $srcStream->close();

        static::assertFalse((new Move($fstab))->execute($src, $dest));

        static::assertNotNull($fstab->for($dest)->status($dest));
        static::assertSame('move-payload', $fstab->for($dest)->readFrom($dest)->content());
    }

    public function test_cross_mount_move_streams_copy_then_removes_source(): void
    {
        $fstab = new FilesystemTable(memory_filesystem(), native_local_filesystem());

        $src = path('memory://src.txt');
        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('move-payload');
        $srcStream->close();

        $destPath = sys_get_temp_dir() . '/flow_move_test_' . bin2hex(random_bytes(4));
        $dest = path('file://' . $destPath);

        static::assertTrue((new Move($fstab))->execute($src, $dest));

        static::assertNull($fstab->for($src)->status($src));
        static::assertSame('move-payload', file_get_contents($destPath));
        @unlink($destPath);
    }

    public function test_same_mount_move_delegates_to_filesystem_mv(): void
    {
        $fstab = new FilesystemTable(native_local_filesystem());

        $tmp = sys_get_temp_dir() . '/flow_move_local_' . bin2hex(random_bytes(4));
        file_put_contents($tmp, 'local-bytes');
        $destPath = $tmp . '.moved';

        static::assertTrue((new Move($fstab))->execute(path('file://' . $tmp), path('file://' . $destPath)));

        static::assertFileDoesNotExist($tmp);
        static::assertSame('local-bytes', file_get_contents($destPath));
        @unlink($destPath);
    }
}
