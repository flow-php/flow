<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Service\Manifest;

use Flow\Website\Service\Manifest\Manifest;
use Flow\Website\Service\Manifest\PackageMeta;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function file_put_contents;
use function json_encode;
use function sys_get_temp_dir;
use function tempnam;

final class PackageMetaTest extends TestCase
{
    /**
     * @param list<array<string, mixed>> $packages
     */
    public function buildMeta(array $packages): PackageMeta
    {
        $path = tempnam(sys_get_temp_dir(), 'flow-manifest-');
        self::assertNotFalse($path);
        file_put_contents($path, json_encode(['packages' => $packages]));

        return new PackageMeta(new Manifest($path));
    }

    public function test_etl_package_uses_core_label_override(): void
    {
        $meta = $this->buildMeta([
            ['name' => 'flow-php/etl', 'path' => 'src/core/etl', 'type' => 'core'],
        ]);

        static::assertSame(['type' => 'Core', 'component' => 'Core'], $meta->forPackage('flow-php/etl'));
    }

    #[TestWith(['core', 'Core'])]
    #[TestWith(['csv', 'CSV'])]
    #[TestWith(['json', 'JSON'])]
    #[TestWith(['parquet', 'Parquet'])]
    #[TestWith(['google-sheet', 'Google Sheet'])]
    #[TestWith(['CSV', 'CSV'])]
    public function test_for_dsl_module_returns_label(string $module, string $expected): void
    {
        $meta = $this->buildMeta([]);

        static::assertSame($expected, $meta->forDslModule($module));
    }

    public function test_for_dsl_module_returns_null_for_unknown_module(): void
    {
        $meta = $this->buildMeta([]);

        static::assertNull($meta->forDslModule('not-a-module'));
    }

    #[TestWith(['flow-php/etl-adapter-csv', 'adapter', 'Adapter', 'CSV'])]
    #[TestWith(['flow-php/etl-adapter-json', 'adapter', 'Adapter', 'JSON'])]
    #[TestWith(['flow-php/etl-adapter-xml', 'adapter', 'Adapter', 'XML'])]
    #[TestWith(['flow-php/etl-adapter-http', 'adapter', 'Adapter', 'HTTP'])]
    #[TestWith(['flow-php/etl-adapter-google-sheet', 'adapter', 'Adapter', 'Google Sheet'])]
    #[TestWith(['flow-php/parquet', 'lib', 'Library', 'Parquet'])]
    #[TestWith(['flow-php/postgresql', 'lib', 'Library', 'PostgreSQL'])]
    #[TestWith(['flow-php/azure-sdk', 'lib', 'Library', 'Azure SDK'])]
    #[TestWith(['flow-php/dremel', 'lib', 'Library', 'Dremel'])]
    #[TestWith(['flow-php/cli', 'cli', 'CLI', 'CLI'])]
    #[TestWith(['flow-php/symfony-postgresql-bundle', 'bridge', 'Bridge', 'Symfony PostgreSQL Bundle'])]
    #[TestWith(['flow-php/psr18-telemetry-bridge', 'bridge', 'Bridge', 'PSR18 Telemetry Bridge'])]
    #[TestWith(['flow-php/telemetry-otlp-bridge', 'bridge', 'Bridge', 'Telemetry OTLP Bridge'])]
    #[TestWith(['flow-php/doctrine-dbal-bulk', 'lib', 'Library', 'Doctrine DBAL Bulk'])]
    #[TestWith(['flow-php/arrow-ext', 'extension', 'Extension', 'Arrow Ext'])]
    public function test_for_package_returns_expected_label(
        string $name,
        string $type,
        string $expectedType,
        string $expectedComponent,
    ): void {
        $meta = $this->buildMeta([
            ['name' => $name, 'path' => 'src/' . $name, 'type' => $type],
        ]);

        static::assertSame(['type' => $expectedType, 'component' => $expectedComponent], $meta->forPackage($name));
    }

    public function test_for_package_returns_null_for_unknown_package(): void
    {
        $meta = $this->buildMeta([]);

        static::assertNull($meta->forPackage('flow-php/missing'));
    }

    public function test_for_package_returns_null_for_unknown_type(): void
    {
        $meta = $this->buildMeta([
            ['name' => 'flow-php/wat', 'path' => 'src/wat', 'type' => 'something-else'],
        ]);

        static::assertNull($meta->forPackage('flow-php/wat'));
    }
}
