<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Operations;

use function Flow\Filesystem\DSL\{memory_filesystem, native_local_filesystem, path};
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Operations\{Copy, Move};
use Flow\Filesystem\Tests\Double\{FailingRmFilesystem, ThrowingSourceFilesystem};
use PHPUnit\Framework\TestCase;

final class MoveTest extends TestCase
{
    public function test_cross_mount_move_propagates_iteration_error_and_closes_streams() : void
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
            self::fail('Expected RuntimeException to be thrown from throwing iterate()');
        } catch (RuntimeException $e) {
            self::assertSame('Throwing source stream failed mid-iterate', $e->getMessage());
        }

        self::assertNotNull($throwing->lastStream);
        self::assertTrue($throwing->lastStream->closed, 'Source stream must be closed by the finally block');
    }

    public function test_cross_mount_move_returns_false_when_source_rm_fails() : void
    {
        $fstab = new FilesystemTable(
            new FailingRmFilesystem(memory_filesystem('src')),
            memory_filesystem('dest'),
        );

        $src = path('src://file.txt');
        $dest = path('dest://file.txt');

        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('move-payload');
        $srcStream->close();

        self::assertFalse((new Move($fstab))->execute($src, $dest));

        self::assertNotNull($fstab->for($dest)->status($dest));
        self::assertSame('move-payload', $fstab->for($dest)->readFrom($dest)->content());
    }

    public function test_cross_mount_move_streams_copy_then_removes_source() : void
    {
        $fstab = new FilesystemTable(memory_filesystem(), native_local_filesystem());

        $src = path('memory://src.txt');
        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('move-payload');
        $srcStream->close();

        $destPath = \sys_get_temp_dir() . '/flow_move_test_' . \bin2hex(\random_bytes(4));
        $dest = path('file://' . $destPath);

        self::assertTrue((new Move($fstab))->execute($src, $dest));

        self::assertNull($fstab->for($src)->status($src));
        self::assertSame('move-payload', \file_get_contents($destPath));
        @\unlink($destPath);
    }

    public function test_same_mount_move_delegates_to_filesystem_mv() : void
    {
        $fstab = new FilesystemTable(native_local_filesystem());

        $tmp = \sys_get_temp_dir() . '/flow_move_local_' . \bin2hex(\random_bytes(4));
        \file_put_contents($tmp, 'local-bytes');
        $destPath = $tmp . '.moved';

        self::assertTrue((new Move($fstab))->execute(path('file://' . $tmp), path('file://' . $destPath)));

        self::assertFileDoesNotExist($tmp);
        self::assertSame('local-bytes', \file_get_contents($destPath));
        @\unlink($destPath);
    }
}
