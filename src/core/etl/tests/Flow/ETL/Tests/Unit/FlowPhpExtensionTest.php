<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowPhpExtension;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

final class FlowPhpExtensionTest extends TestCase
{
    public function test_an_extension_that_is_not_loaded_is_not_available(): void
    {
        static::assertFalse((new FlowPhpExtension(false, null))->available());
    }

    public function test_a_loaded_extension_of_the_expected_abi_is_available(): void
    {
        static::assertTrue((new FlowPhpExtension(true, FlowPhpExtension::ABI))->available());
    }

    public function test_a_loaded_extension_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'The loaded flow_php extension (0.46.0, ABI 2) does not match flow-php/etl (ABI 1). Rebuild or upgrade '
            . 'flow_php to the version of flow-php/etl, or stop loading it to use the PHP implementation.',
        );

        (new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'))->available();
    }

    public function test_a_loaded_extension_without_an_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The loaded flow_php extension (0.44.1, ABI none) does not match flow-php/etl');

        (new FlowPhpExtension(true, null, '0.44.1'))->available();
    }

    public function test_detect_reports_whether_flow_php_is_loaded(): void
    {
        $extension = FlowPhpExtension::detect();

        static::assertSame(extension_loaded('flow_php'), $extension->loaded);
        static::assertSame(extension_loaded('flow_php') ? FlowPhpExtension::ABI : null, $extension->abi);
    }
}
