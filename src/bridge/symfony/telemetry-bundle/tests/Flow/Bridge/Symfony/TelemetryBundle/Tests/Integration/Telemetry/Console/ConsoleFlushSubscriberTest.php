<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Console\ConsoleFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command\TestCommand;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanExporter;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

#[CoversClass(ConsoleFlushSubscriber::class)]
final class ConsoleFlushSubscriberTest extends KernelTestCase
{
    #[\Override]
    protected function tearDown() : void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_flush_is_called_on_terminate() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => true],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $application->add(new TestCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:command']);
        $output = new BufferedOutput();

        $exitCode = $application->run($input, $output);

        self::assertSame(0, $exitCode);

        $container = $this->getContainer();
        /** @var MemorySpanExporter $exporter */
        $exporter = $container->get('flow.telemetry.tracer_provider.processor.exporter');
        $spans = $exporter->spans();

        self::assertCount(1, $spans, 'Spans should be exported after console terminate when flush is called');
    }

    public function test_flush_is_not_called_when_console_instrumentation_is_disabled() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $application->add(new TestCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:command']);
        $output = new BufferedOutput();

        $application->run($input, $output);

        $container = $this->getContainer();
        /** @var MemorySpanExporter $exporter */
        $exporter = $container->get('flow.telemetry.tracer_provider.processor.exporter');
        $spans = $exporter->spans();

        self::assertCount(0, $spans, 'No spans should be exported when instrumentation is disabled');
    }
}
