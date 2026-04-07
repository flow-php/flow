<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration\Command;

use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\FilesystemBundle\Command\{CpCommand, FstabResolver, LsCommand, MvCommand};
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration\KernelTestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class FilesystemCommandsIntegrationTest extends KernelTestCase
{
    /** @var list<string> */
    private array $tempPaths = [];

    protected function tearDown() : void
    {
        foreach ($this->tempPaths as $dir) {
            $this->rmRf($dir);
        }

        $this->tempPaths = [];

        parent::tearDown();
    }

    public function test_cp_between_protocols_in_same_fstab() : void
    {
        $resolver = $this->bootWithMultiFstab();

        $table = $resolver->resolve(null);
        $stream = $table->for(path('memory://src.txt'))->writeTo(path('memory://src.txt'));
        $stream->append('integration-payload');
        $stream->close();

        $dir = $this->makeTempDir();
        $destUri = 'file://' . $dir . '/out.txt';

        $tester = new CommandTester(new CpCommand($resolver));
        $exit = $tester->execute(['source' => 'memory://src.txt', 'destination' => $destUri]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('integration-payload', \file_get_contents($dir . '/out.txt'));
        self::assertNotNull($table->for(path('memory://src.txt'))->status(path('memory://src.txt')));
    }

    public function test_cp_fails_when_protocol_missing_in_chosen_fstab() : void
    {
        $resolver = $this->bootWithMultiFstab();

        $secondary = $resolver->resolve('secondary');
        $stream = $secondary->for(path('memory://only.txt'))->writeTo(path('memory://only.txt'));
        $stream->append('x');
        $stream->close();

        $tester = new CommandTester(new CpCommand($resolver));
        $exit = $tester->execute([
            'source' => 'memory://only.txt',
            'destination' => 'file:///tmp/nope.txt',
            '--fstab' => 'secondary',
        ]);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('secondary', $tester->getDisplay());
    }

    public function test_fstab_option_routes_to_secondary() : void
    {
        $resolver = $this->bootWithMultiFstab();

        $secondary = $resolver->resolve('secondary');
        $stream = $secondary->for(path('memory://data/hello.txt'))->writeTo(path('memory://data/hello.txt'));
        $stream->append('secondary-only');
        $stream->close();

        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://data', '--fstab' => 'secondary']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertStringContainsString('memory://data/hello.txt', $tester->getDisplay());
    }

    public function test_mv_removes_source_after_success() : void
    {
        $resolver = $this->bootWithMultiFstab();

        $table = $resolver->resolve(null);
        $stream = $table->for(path('memory://src.txt'))->writeTo(path('memory://src.txt'));
        $stream->append('moving');
        $stream->close();

        $dir = $this->makeTempDir();
        $destUri = 'file://' . $dir . '/moved.txt';

        $tester = new CommandTester(new MvCommand($resolver));
        $exit = $tester->execute(['source' => 'memory://src.txt', 'destination' => $destUri]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('moving', \file_get_contents($dir . '/moved.txt'));
        self::assertNull($table->for(path('memory://src.txt'))->status(path('memory://src.txt')));
    }

    private function bootWithMultiFstab() : FstabResolver
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => ['filesystems' => ['memory' => [], 'file' => []]],
                        'secondary' => ['filesystems' => ['memory' => []]],
                    ],
                ]);

                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->addCompilerPass(new class implements CompilerPassInterface {
                        public function process(ContainerBuilder $container) : void
                        {
                            if ($container->hasDefinition('.flow_filesystem.command.fstab_resolver')) {
                                $container->getDefinition('.flow_filesystem.command.fstab_resolver')->setPublic(true);
                            }
                        }
                    });
                });
            },
        ]);

        /** @var FstabResolver $resolver */
        $resolver = $this->getContainer()->get('.flow_filesystem.command.fstab_resolver');

        return $resolver;
    }

    private function makeTempDir() : string
    {
        $dir = \sys_get_temp_dir() . '/flow_fs_cli_int_' . \bin2hex(\random_bytes(6));
        \mkdir($dir, 0o777, true);
        $this->tempPaths[] = $dir;

        return $dir;
    }

    private function rmRf(string $dir) : void
    {
        if (!\is_dir($dir)) {
            return;
        }

        foreach (\scandir($dir) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $p = $dir . '/' . $entry;

            if (\is_dir($p)) {
                $this->rmRf($p);
            } else {
                @\unlink($p);
            }
        }

        @\rmdir($dir);
    }
}
