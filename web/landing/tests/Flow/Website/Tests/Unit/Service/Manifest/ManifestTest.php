<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Service\Manifest;

use Flow\Website\Service\Manifest\Manifest;
use PHPUnit\Framework\TestCase;

final class ManifestTest extends TestCase
{
    public function test_all_returns_packages_indexed_by_name(): void
    {
        $manifest = new Manifest($this->writeManifest([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
            ['name' => 'flow-php/etl-adapter-csv', 'path' => 'src/adapter/etl-adapter-csv', 'type' => 'adapter'],
        ]));

        $all = $manifest->all();

        static::assertArrayHasKey('flow-php/etl', $all);
        static::assertArrayHasKey('flow-php/etl-adapter-csv', $all);
        static::assertSame('core', $all['flow-php/etl']['type']);
    }

    public function test_by_name_returns_null_for_unknown_package(): void
    {
        $manifest = new Manifest($this->writeManifest([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]));

        static::assertNull($manifest->byName('flow-php/nonexistent'));
    }

    public function test_by_name_returns_package_entry(): void
    {
        $manifest = new Manifest($this->writeManifest([
            ['name' => 'flow-php/parquet', 'path' => 'src/lib/parquet', 'type' => 'lib'],
        ]));

        $entry = $manifest->byName('flow-php/parquet');

        static::assertNotNull($entry);
        static::assertSame('flow-php/parquet', $entry['name']);
        static::assertSame('lib', $entry['type']);
    }

    public function test_load_is_cached_between_calls(): void
    {
        $path = $this->writeManifest([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);
        $manifest = new Manifest($path);

        $manifest->all();
        \unlink($path);

        // Second call must succeed from cache; would throw if it re-read from disk.
        static::assertNotNull($manifest->byName('flow-php/etl'));
    }

    public function test_skips_entries_with_non_string_name(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'flow-manifest-');
        static::assertNotFalse($path);
        \file_put_contents($path, \json_encode([
            'packages' => [
                ['name' => 'flow-php/etl', 'type' => 'core'],
                ['type' => 'lib'],
                ['name' => 123, 'type' => 'lib'],
            ],
        ]));

        $manifest = new Manifest($path);
        $all = $manifest->all();

        static::assertCount(1, $all);
        static::assertArrayHasKey('flow-php/etl', $all);
        \unlink($path);
    }

    public function test_throws_on_invalid_json(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'flow-manifest-');
        static::assertNotFalse($path);
        \file_put_contents($path, '{not json');

        $manifest = new Manifest($path);

        try {
            $this->expectException(\JsonException::class);
            $manifest->all();
        } finally {
            \unlink($path);
        }
    }

    public function test_throws_when_file_missing(): void
    {
        $manifest = new Manifest('/no/such/manifest.json');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Flow manifest not found');

        $manifest->all();
    }

    /**
     * @param list<array<string, mixed>> $packages
     */
    public function writeManifest(array $packages): string
    {
        $path = \tempnam(\sys_get_temp_dir(), 'flow-manifest-');
        self::assertNotFalse($path);
        \file_put_contents($path, \json_encode(['packages' => $packages]));

        return $path;
    }
}
