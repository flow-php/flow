<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Command\CatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\CpCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\FstabResolver;
use Flow\Bridge\Symfony\FilesystemBundle\Command\LsCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\MvCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\RmCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\StatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\TouchCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration\KernelTestCase;
use Flow\Filesystem\FilesystemTable;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_filter;
use function array_values;
use function bin2hex;
use function explode;
use function file_exists;
use function file_get_contents;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function json_decode;
use function mkdir;
use function random_bytes;
use function rmdir;
use function scandir;
use function sprintf;
use function substr_count;
use function sys_get_temp_dir;
use function trim;
use function unlink;

use const JSON_THROW_ON_ERROR;

final class FilesystemCommandsIntegrationTest extends KernelTestCase
{
    /** @var list<string> */
    private array $tempPaths = [];

    protected function tearDown(): void
    {
        foreach ($this->tempPaths as $dir) {
            $this->rmRf($dir);
        }

        $this->tempPaths = [];

        parent::tearDown();
    }

    public function test_cat_prints_file_contents(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);
        $this->seed($table, 'memory://cat.txt', 'line-1');

        $tester = new CommandTester(new CatCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://cat.txt']);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertStringContainsString('line-1', $tester->getDisplay());
    }

    public function test_cp_between_protocols_in_same_fstab(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);
        $this->seed($table, 'memory://src.txt', 'integration-payload');

        $dir = $this->makeTempDir();
        $destUri = 'file://' . $dir . '/out.txt';

        $tester = new CommandTester(new CpCommand($resolver));
        $exit = $tester->execute(['source' => 'memory://src.txt', 'destination' => $destUri]);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertSame('integration-payload', file_get_contents($dir . '/out.txt'));
        static::assertNotNull($table->for(path('memory://src.txt'))->status(path('memory://src.txt')));
    }

    public function test_cp_fails_when_protocol_missing_in_chosen_fstab(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve('secondary'), 'memory://only.txt', 'x');

        $tester = new CommandTester(new CpCommand($resolver));
        $exit = $tester->execute([
            'source' => 'memory://only.txt',
            'destination' => 'file:///tmp/nope.txt',
            '--fstab' => 'secondary',
        ]);

        static::assertSame(Command::FAILURE, $exit);
        static::assertStringContainsString('secondary', $tester->getDisplay());
    }

    public function test_ls_default_emits_size_and_modified_columns(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve(null), 'memory://long/a.txt', 'hello');

        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://long']);

        static::assertSame(Command::SUCCESS, $exit);
        $display = $tester->getDisplay();
        static::assertStringContainsString('Type', $display);
        static::assertStringContainsString('Size', $display);
        static::assertStringContainsString('Modified', $display);
        static::assertStringContainsString('5 B', $display);
    }

    public function test_ls_recursive_lists_top_level_files_on_a_memory_filesystem(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve(null), 'memory://tree/top.txt', 'top');
        $this->seed($resolver->resolve(null), 'memory://tree/nested/deep.txt', 'deep');

        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://tree', '--recursive' => true]);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertStringContainsString('top.txt', $tester->getDisplay());
        static::assertStringContainsString('deep.txt', $tester->getDisplay());
    }

    public function test_ls_does_not_prompt_when_limit_exactly_equals_page_size(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 25; $i++) {
            $this->seed($table, sprintf('memory://cap/f%03d.txt', $i), 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://cap', '--limit' => '10', '--page-size' => '10'], ['interactive' => true]);

        $display = $tester->getDisplay();
        static::assertStringNotContainsString('Show next', $display);
        static::assertSame(10, substr_count($display, 'memory://cap/'));
        static::assertStringContainsString('truncated at 10 entries', $display);
    }

    public function test_ls_fails_on_negative_offset(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://', '--offset' => '-1']);

        static::assertSame(Command::FAILURE, $exit);
        static::assertStringContainsString('--offset must be a non-negative integer', $tester->getDisplay());
    }

    public function test_ls_fails_on_non_positive_limit(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://', '--limit' => '0']);

        static::assertSame(Command::FAILURE, $exit);
        static::assertStringContainsString('--limit must be a positive integer', $tester->getDisplay());
    }

    public function test_ls_flows_all_pages_without_prompt_when_not_interactive(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 25; $i++) {
            $this->seed($table, sprintf('memory://flow/f%03d.txt', $i), 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://flow'], ['interactive' => false]);

        $display = $tester->getDisplay();
        static::assertStringNotContainsString('Show next', $display);
        static::assertSame(25, substr_count($display, 'memory://flow/'));
    }

    public function test_ls_fstab_option_routes_to_secondary(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve('secondary'), 'memory://data/hello.txt', 'secondary-only');

        $tester = new CommandTester(new LsCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://data', '--fstab' => 'secondary']);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertStringContainsString('memory://data/hello.txt', $tester->getDisplay());
    }

    public function test_ls_json_format_emits_ndjson_per_entry(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);
        $this->seed($table, 'memory://j/a.txt', 'A');
        $this->seed($table, 'memory://j/b.txt', 'BB');

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://j', '--format' => 'json']);

        $lines = array_values(array_filter(explode("\n", trim($tester->getDisplay()))));
        static::assertCount(2, $lines);

        /** @var array{uri: string, size: null|int} $first */
        $first = json_decode($lines[0], true, flags: JSON_THROW_ON_ERROR);
        static::assertSame('memory://j/a.txt', $first['uri']);
        static::assertSame(1, $first['size']);
    }

    public function test_ls_limit_truncates_and_warns(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 5; $i++) {
            $this->seed($table, 'memory://lim/f' . $i . '.txt', 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://lim', '--limit' => '2']);

        $display = $tester->getDisplay();
        static::assertSame(2, substr_count($display, 'memory://lim/'));
        static::assertStringContainsString('truncated at 2 entries', $display);
    }

    public function test_ls_offset_skips_first_n_entries(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 5; $i++) {
            $this->seed($table, 'memory://off/f' . $i . '.txt', 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://off', '--offset' => '2', '--limit' => '10']);

        $display = $tester->getDisplay();
        static::assertSame(3, substr_count($display, 'memory://off/'));
        static::assertStringNotContainsString('memory://off/f0.txt', $display);
        static::assertStringNotContainsString('memory://off/f1.txt', $display);
        static::assertStringContainsString('memory://off/f2.txt', $display);
    }

    public function test_ls_prompts_between_pages_on_interactive_tty(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 25; $i++) {
            $this->seed($table, sprintf('memory://pg/f%03d.txt', $i), 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->setInputs(['no']);
        $tester->execute(['path' => 'memory://pg'], ['interactive' => true]);

        $display = $tester->getDisplay();
        static::assertStringContainsString('Show next 10 entries?', $display);
        static::assertSame(10, substr_count($display, 'memory://pg/'));
    }

    public function test_ls_returns_all_entries_in_single_page_when_under_page_size(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);

        for ($i = 0; $i < 3; $i++) {
            $this->seed($table, 'memory://nl/f' . $i . '.txt', 'x');
        }

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://nl']);

        static::assertSame(3, substr_count($tester->getDisplay(), 'memory://nl/'));
        static::assertStringNotContainsString('truncated', $tester->getDisplay());
    }

    public function test_ls_short_drops_metadata_columns(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve(null), 'memory://short/a.txt', 'hello');

        $tester = new CommandTester(new LsCommand($resolver));
        $tester->execute(['path' => 'memory://short', '--short' => true]);

        $display = $tester->getDisplay();
        static::assertStringNotContainsString('Type', $display);
        static::assertStringNotContainsString('Modified', $display);
        static::assertStringContainsString('memory://short/a.txt', $display);
    }

    public function test_mv_removes_source_after_success(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);
        $this->seed($table, 'memory://src.txt', 'moving');

        $dir = $this->makeTempDir();
        $destUri = 'file://' . $dir . '/moved.txt';

        $tester = new CommandTester(new MvCommand($resolver));
        $exit = $tester->execute(['source' => 'memory://src.txt', 'destination' => $destUri]);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertSame('moving', file_get_contents($dir . '/moved.txt'));
        static::assertNull($table->for(path('memory://src.txt'))->status(path('memory://src.txt')));
    }

    public function test_rm_removes_existing_file(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $table = $resolver->resolve(null);
        $this->seed($table, 'memory://rm/f.txt', 'x');

        $tester = new CommandTester(new RmCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://rm/f.txt']);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertNull($table->for(path('memory://rm/f.txt'))->status(path('memory://rm/f.txt')));
    }

    public function test_stat_fails_on_missing_path(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $tester = new CommandTester(new StatCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://absent.txt']);

        static::assertSame(Command::FAILURE, $exit);
        static::assertStringContainsString('not found', $tester->getDisplay());
    }

    public function test_stat_json_format_includes_metadata(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve(null), 'memory://stat.bin', 'abcd');

        $tester = new CommandTester(new StatCommand($resolver));
        $tester->execute(['path' => 'memory://stat.bin', '--format' => 'json']);

        /** @var array{uri: string, type: string, size: null|int, modified: null|string} $data */
        $data = json_decode(trim($tester->getDisplay()), true, flags: JSON_THROW_ON_ERROR);
        static::assertSame('memory://stat.bin', $data['uri']);
        static::assertSame(4, $data['size']);
        static::assertNotNull($data['modified']);
    }

    public function test_stat_prints_size_and_modified(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $this->seed($resolver->resolve(null), 'memory://stat.txt', 'abcd');

        $tester = new CommandTester(new StatCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://stat.txt']);

        static::assertSame(Command::SUCCESS, $exit);
        $display = $tester->getDisplay();
        static::assertStringContainsString('memory://stat.txt', $display);
        static::assertStringContainsString('file', $display);
        static::assertStringContainsString('Size', $display);
        static::assertStringContainsString('4', $display);
        static::assertStringContainsString('Modified', $display);
    }

    public function test_stat_rejects_pattern_paths(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $tester = new CommandTester(new StatCommand($resolver));
        $exit = $tester->execute(['path' => 'memory://*.txt']);

        static::assertSame(Command::FAILURE, $exit);
        static::assertStringContainsString('Pattern paths are not supported by stat', $tester->getDisplay());
    }

    public function test_touch_creates_empty_file(): void
    {
        $resolver = $this->bootWithMultiFstab();
        $dir = $this->makeTempDir();
        $target = 'file://' . $dir . '/touched.txt';

        $tester = new CommandTester(new TouchCommand($resolver));
        $exit = $tester->execute(['path' => $target]);

        static::assertSame(Command::SUCCESS, $exit);
        static::assertTrue(file_exists($dir . '/touched.txt'));
        static::assertSame('', file_get_contents($dir . '/touched.txt'));
    }

    private function bootWithMultiFstab(): FstabResolver
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_filesystem', [
                    'fstabs' => [
                        'default' => [
                            'filesystems' => ['memory' => ['type' => 'memory'], 'file' => ['type' => 'file']],
                        ],
                        'secondary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
                    ],
                ]);

                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->addCompilerPass(new class implements CompilerPassInterface {
                        public function process(ContainerBuilder $container): void
                        {
                            if ($container->hasDefinition('.flow.filesystem.command.fstab_resolver')) {
                                $container->getDefinition('.flow.filesystem.command.fstab_resolver')->setPublic(true);
                            }
                        }
                    });
                });
            },
        ]);

        $resolver = $this->getContainer()->get('.flow.filesystem.command.fstab_resolver');

        if (!$resolver instanceof FstabResolver) {
            static::fail('Expected FstabResolver service to be available in the container.');
        }

        return $resolver;
    }

    private function makeTempDir(): string
    {
        $dir = sys_get_temp_dir() . '/flow_fs_cli_int_' . bin2hex(random_bytes(6));
        mkdir($dir, 0o777, true);
        $this->tempPaths[] = $dir;

        return $dir;
    }

    private function rmRf(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }

        foreach (scandir($dir) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $p = $dir . '/' . $entry;

            if (is_dir($p)) {
                $this->rmRf($p);
            } else {
                @unlink($p);
            }
        }

        @rmdir($dir);
    }

    private function seed(FilesystemTable $table, string $uri, string $content): void
    {
        $path = path($uri);
        $stream = $table->for($path)->writeTo($path);
        $stream->append($content);
        $stream->close();
    }
}
