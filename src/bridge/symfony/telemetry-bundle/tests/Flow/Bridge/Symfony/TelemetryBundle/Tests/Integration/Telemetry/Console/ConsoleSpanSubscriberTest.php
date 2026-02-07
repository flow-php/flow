<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Console\ConsoleSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command\{FailingCommand, TestCommand};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

#[CoversClass(ConsoleSpanSubscriber::class)]
final class ConsoleSpanSubscriberTest extends KernelTestCase
{
    #[\Override]
    protected function tearDown() : void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_does_not_trace_when_disabled() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
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
        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(0, $spans);
    }

    public function test_excludes_command_with_exact_match() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => [
                            'enabled' => true,
                            'exclude_commands' => ['test:command'],
                        ],
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
        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(0, $spans);
    }

    public function test_excludes_command_with_regex_pattern() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => [
                            'enabled' => true,
                            'exclude_commands' => ['/^test:.*/'],
                        ],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $application->add(new TestCommand());
        $application->add(new FailingCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:command']);
        $output = new BufferedOutput();

        $application->run($input, $output);

        $input = new ArrayInput(['command' => 'test:failing']);
        $application->run($input, $output);

        $container = $this->getContainer();
        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(0, $spans, 'Both test:command and test:failing should be excluded by regex');
    }

    public function test_traces_failing_console_command() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => true],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $application->add(new FailingCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:failing']);
        $output = new BufferedOutput();

        $exitCode = $application->run($input, $output);

        self::assertSame(1, $exitCode);

        $container = $this->getContainer();
        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('test:failing', $span->name());

        $attributes = $span->attributes();
        self::assertSame(1, $attributes['process.exit_code']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertSame('Exit code: 1', $status->description);
    }

    public function test_traces_successful_console_command() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
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
        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('test:command', $span->name());
        self::assertSame(SpanKind::INTERNAL, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('test:command', $attributes['command.name']);
        self::assertSame(TestCommand::class, $attributes['command.class']);
        self::assertSame(0, $attributes['process.exit_code']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isOk());
    }
}
