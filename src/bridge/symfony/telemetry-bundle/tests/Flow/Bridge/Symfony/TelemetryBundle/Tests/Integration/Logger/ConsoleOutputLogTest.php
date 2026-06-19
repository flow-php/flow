<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Logger;

use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleOutputLogProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command\LoggingCommand;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Telemetry;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

#[CoversClass(ConsoleOutputLogProcessor::class)]
final class ConsoleOutputLogTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_default_verbosity_shows_errors_only_and_exports_every_record(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'logger_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
                        'console_output' => ['enabled' => true],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        /** @var Telemetry $telemetry */
        $telemetry = $this->getContainer()->get('flow.telemetry');
        $application = new Application($kernel);
        $this->addCommand($application, new LoggingCommand($telemetry));
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $output = new BufferedOutput();
        $application->run(new ArrayInput(['command' => 'test:logging']), $output);
        $written = $output->fetch();

        static::assertStringContainsString('error-message', $written);
        static::assertStringNotContainsString('warn-message', $written);
        static::assertStringNotContainsString('info-message', $written);
        static::assertStringNotContainsString('debug-message', $written);
        static::assertStringNotContainsString('trace-message', $written);

        /** @var MemoryLogProcessor $processor */
        $processor = $this->getContainer()->get('flow.telemetry.logger_provider.processor');
        static::assertSame(5, $processor->countLogs());
    }

    public function test_quiet_verbosity_shows_errors_only(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'logger_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
                        'console_output' => ['enabled' => true],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        /** @var Telemetry $telemetry */
        $telemetry = $this->getContainer()->get('flow.telemetry');
        $application = new Application($kernel);
        $this->addCommand($application, new LoggingCommand($telemetry));
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $output = new BufferedOutput();
        $application->run(new ArrayInput(['command' => 'test:logging', '-q' => true]), $output);
        $written = $output->fetch();

        static::assertStringContainsString('error-message', $written);
        static::assertStringNotContainsString('warn-message', $written);
    }

    public function test_verbose_verbosity_shows_warnings_but_not_info(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'logger_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
                        'console_output' => ['enabled' => true],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        /** @var Telemetry $telemetry */
        $telemetry = $this->getContainer()->get('flow.telemetry');
        $application = new Application($kernel);
        $this->addCommand($application, new LoggingCommand($telemetry));
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $output = new BufferedOutput();
        $application->run(new ArrayInput(['command' => 'test:logging', '-v' => true]), $output);
        $written = $output->fetch();

        static::assertStringContainsString('warn-message', $written);
        static::assertStringNotContainsString('info-message', $written);
    }
}
