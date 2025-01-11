<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\Manifest;
use Flow\ETL\Tests\FlowTestCase;

final class ManifestTest extends FlowTestCase
{
    public function test_manifest() : void
    {
        $manifest = Manifest::fromJson(\file_get_contents($this->repositoryRoot() . '/manifest.json'));
        self::assertCount(28, $manifest->packages);

        foreach ($manifest->packages as $package) {
            self::assertFileExists($this->repositoryRoot() . '/' . $package->path);
        }
    }
}
