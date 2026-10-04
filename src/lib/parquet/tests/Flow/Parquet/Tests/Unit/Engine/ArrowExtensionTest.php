<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\ArrowExtension;
use Flow\Parquet\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

final class ArrowExtensionTest extends TestCase
{
    public function test_an_extension_that_is_not_loaded_is_not_available(): void
    {
        static::assertFalse((new ArrowExtension(false, null))->available());
    }

    public function test_a_loaded_extension_of_the_expected_abi_is_available(): void
    {
        static::assertTrue((new ArrowExtension(true, ArrowExtension::ABI))->available());
    }

    public function test_a_loaded_extension_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'The loaded arrow extension (0.46.0, ABI 2) does not match flow-php/parquet (ABI 1). Rebuild or upgrade '
            . 'arrow-ext to the version of flow-php/parquet, or stop loading it to use the PHP implementation.',
        );

        (new ArrowExtension(true, ArrowExtension::ABI + 1, '0.46.0'))->available();
    }

    public function test_a_loaded_extension_without_an_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The loaded arrow extension (0.44.1, ABI none) does not match flow-php/parquet');

        (new ArrowExtension(true, null, '0.44.1'))->available();
    }

    public function test_detect_reports_whether_arrow_is_loaded(): void
    {
        $extension = ArrowExtension::detect();

        static::assertSame(extension_loaded('arrow'), $extension->loaded);
        static::assertSame(extension_loaded('arrow') ? ArrowExtension::ABI : null, $extension->abi);
    }
}
