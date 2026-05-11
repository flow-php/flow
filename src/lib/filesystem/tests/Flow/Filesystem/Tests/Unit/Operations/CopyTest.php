<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Operations;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Operations\Copy;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;

final class CopyTest extends TestCase
{
    public function test_copies_across_mounts_and_leaves_source_in_place(): void
    {
        $fstab = new FilesystemTable(memory_filesystem(), native_local_filesystem());

        $src = path('memory://src.txt');
        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('copy-payload');
        $srcStream->close();

        $destPath = \sys_get_temp_dir() . '/flow_copy_test_' . \bin2hex(\random_bytes(4));
        $dest = path('file://' . $destPath);

        static::assertTrue((new Copy($fstab))->execute($src, $dest));

        static::assertNotNull($fstab->for($src)->status($src));
        static::assertSame('copy-payload', \file_get_contents($destPath));
        @\unlink($destPath);
    }

    public function test_copies_within_a_single_mount(): void
    {
        $fstab = new FilesystemTable(memory_filesystem());

        $src = path('memory://a.txt');
        $srcStream = $fstab->for($src)->writeTo($src);
        $srcStream->append('intra-copy');
        $srcStream->close();

        $dest = path('memory://b.txt');

        static::assertTrue((new Copy($fstab))->execute($src, $dest));

        static::assertNotNull($fstab->for($src)->status($src));
        static::assertNotNull($fstab->for($dest)->status($dest));
    }
}
