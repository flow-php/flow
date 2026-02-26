<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

trait SnapshotTestTrait
{
    private function assertMatchesSnapshot(string $actual, string $fixturePath) : void
    {
        $fs = native_local_filesystem();
        $path = path($fixturePath);

        if (\getenv('UPDATE_SNAPSHOTS') === '1') {
            $fs->writeTo($path)->append($actual)->close();
            self::markTestSkipped('Snapshot updated: ' . $fixturePath);
        }

        $status = $fs->status($path);

        if ($status === null) {
            $fs->writeTo($path)->append($actual)->close();
            self::fail('Snapshot created: ' . $fixturePath . ' - run test again to verify');
        }

        self::assertSame($fs->readFrom($path)->content(), $actual);
    }
}
