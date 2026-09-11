<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Datasets;

use Flow\Benchmarks\Datasets\FixtureFingerprint;
use Flow\Benchmarks\Datasets\FixtureFormat;
use Flow\Benchmarks\Datasets\FixturePath;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_unique;
use function count;

final class FixtureFingerprintTest extends TestCase
{
    public function test_csv_and_json_fingerprints_differ(): void
    {
        static::assertNotSame(
            (new FixtureFingerprint(FixtureFormat::csv))->value(),
            (new FixtureFingerprint(FixtureFormat::json))->value(),
        );
    }

    public function test_fingerprint_is_eight_lowercase_hex_characters(): void
    {
        foreach (FixtureFormat::cases() as $format) {
            static::assertMatchesRegularExpression('/^[0-9a-f]{8}$/', (new FixtureFingerprint($format))->value());
        }
    }

    /**
     * json and json_lines deliberately share a fingerprint, so the extension is the only thing keeping
     * their paths apart - a wrong arm here would make one silently overwrite the other.
     */
    public function test_every_format_maps_to_its_own_extension(): void
    {
        static::assertSame(
            ['csv', 'xlsx', 'floe', 'json', 'jsonl', 'parquet', 'txt', 'xml'],
            array_map(static fn(FixtureFormat $format): string => $format->extension(), FixtureFormat::cases()),
        );
    }

    public function test_no_two_formats_resolve_to_the_same_path(): void
    {
        $paths = array_map(static fn(FixtureFormat $format): string => (new FixturePath(
            'orders',
            1,
            $format,
        ))->path(), FixtureFormat::cases());

        static::assertCount(count($paths), array_unique($paths));
    }

    public function test_json_and_json_lines_share_a_fingerprint(): void
    {
        static::assertSame(
            (new FixtureFingerprint(FixtureFormat::json))->value(),
            (new FixtureFingerprint(FixtureFormat::json_lines))->value(),
        );
    }

    public function test_parquet_and_floe_share_a_fingerprint(): void
    {
        static::assertSame(
            (new FixtureFingerprint(FixtureFormat::parquet))->value(),
            (new FixtureFingerprint(FixtureFormat::floe))->value(),
        );
    }
}
