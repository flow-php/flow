<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPStan\Types\Tests\Unit;

use PHPStan\Testing\TypeInferenceTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function dirname;
use function is_file;

final class StructureTypeReturnTypeExtensionTest extends TypeInferenceTestCase
{
    /**
     * @return iterable<mixed>
     */
    public static function dataFileAsserts(): iterable
    {
        yield from self::gatherAssertTypes(__DIR__ . '/data/type_structure.php');
    }

    /**
     * @return array<string>
     */
    public static function getAdditionalConfigFiles(): array
    {
        $dir = __DIR__;

        while (!is_file($dir . '/extension.neon') && $dir !== dirname($dir)) {
            $dir = dirname($dir);
        }

        return [$dir . '/extension.neon'];
    }

    #[DataProvider('dataFileAsserts')]
    public function test_file_asserts(string $assertType, string $file, mixed ...$args): void
    {
        $this->assertFileAsserts($assertType, $file, ...$args);
    }
}
