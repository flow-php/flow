<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\Manifest;
use Flow\ETL\Tests\FlowTestCase;

use function file_get_contents;

final class ManifestTest extends FlowTestCase
{
    public function test_manifest(): void
    {
        $json = file_get_contents($this->repositoryRoot() . '/manifest.json');
        static::assertNotFalse($json);

        $manifest = Manifest::fromJson($json);
        static::assertCount(54, $manifest->packages);

        foreach ($manifest->packages as $package) {
            static::assertFileExists($this->repositoryRoot() . '/' . $package->path);
        }
    }
}
