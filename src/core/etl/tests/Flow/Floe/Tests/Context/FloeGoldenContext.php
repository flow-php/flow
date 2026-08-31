<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Tests\Mother\RowsMother;

use function Flow\Filesystem\DSL\path;

final class FloeGoldenContext
{
    public const string VERSION_DIRECTORY = 'v2';

    /**
     * @return array<string, list<Rows>>
     */
    public static function batches(): array
    {
        return [
            'all-entry-types' => [RowsMother::withAllEntryTypes()],
            'heterogeneous' => [RowsMother::heterogeneous()],
            'empty' => [RowsMother::empty()],
        ];
    }

    public static function directory(string $version = self::VERSION_DIRECTORY): Path
    {
        return path(__DIR__ . '/../Fixtures/' . $version);
    }

    public static function path(string $name, string $version = self::VERSION_DIRECTORY): Path
    {
        return self::directory($version)->suffix('/' . $name . '.floe');
    }

    /**
     * Rewrites every golden file from the batches above. Committed on purpose: fixtures are only
     * trustworthy when the thing that made them is reviewable, and phase 5 of the format bump has
     * to be reproducible by someone who is not the author.
     */
    public static function regenerate(string $version = self::VERSION_DIRECTORY): void
    {
        $filesystem = new NativeLocalFilesystem();

        foreach (self::batches() as $name => $batches) {
            $path = self::path($name, $version);

            if ($filesystem->status($path) !== null) {
                $filesystem->rm($path);
            }

            FloeEngineContext::writeAll(
                FloeEngineContext::phpWriter($filesystem, self::schema($batches)),
                $path,
                $batches,
            );
        }
    }

    /**
     * @param list<Rows> $batches
     */
    public static function schema(array $batches): Schema
    {
        return $batches[0]->schema();
    }
}
