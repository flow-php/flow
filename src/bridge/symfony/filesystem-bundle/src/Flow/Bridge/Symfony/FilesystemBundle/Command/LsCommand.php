<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_boolean, type_null, type_string, type_union};
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\KeepAll;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:ls', description: 'List files under a URI on a configured fstab.', aliases: ['flow:fs:ls'])]
final class LsCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    protected function configure() : void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'Directory URI, e.g. memory://data or file:///tmp')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->addOption('recursive', 'r', InputOption::VALUE_NONE, 'Recurse into subdirectories.')
            ->addOption('long', 'l', InputOption::VALUE_NONE, 'Show type and size columns.')
            ->addOption('format', null, InputOption::VALUE_REQUIRED, 'Output format: "table" (default) or "json".', 'table')
            ->setHelp(<<<'HELP'
                Lists entries under a directory URI on the chosen fstab.

                Paths must be full URIs in the form <protocol>://<path>. The default fstab is
                used unless --fstab is provided. Use --recursive to walk subdirectories,
                --long for size+type columns, and --format=json for raw JSON output.
                HELP);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));
            $recursive = type_boolean()->assert($input->getOption('recursive'));
            $long = type_boolean()->assert($input->getOption('long'));
            $format = type_string()->assert($input->getOption('format'));

            if (!\in_array($format, ['table', 'json'], true)) {
                $io->getErrorStyle()->error(\sprintf('Unsupported --format "%s". Use "table" or "json".', $format));

                return Command::FAILURE;
            }

            $table = $this->resolver->resolve($fstabName);
            $userPath = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($userPath);

            $listPath = $userPath->isPattern()
                ? $userPath
                : Path::from(\rtrim($userPath->uri(), '/') . ($recursive ? '/**/*' : '/*'));

            $rows = [];

            foreach ($filesystem->list($listPath, new KeepAll()) as $status) {
                $type = $status->isFile() ? 'file' : 'directory';
                $size = null;

                if ($long && $status->isFile()) {
                    $size = $filesystem->readFrom($status->path)->size();
                }

                $rows[] = [
                    'uri' => $status->path->uri(),
                    'type' => $type,
                    'size' => $size,
                ];
            }
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        if ($format === 'json') {
            $output->writeln((string) \json_encode($rows));

            return Command::SUCCESS;
        }

        if ($long) {
            $io->table(
                ['Type', 'Size', 'URI'],
                \array_map(static fn (array $row) : array => [$row['type'], $row['size'] ?? '-', $row['uri']], $rows),
            );
        } else {
            $io->table(['URI'], \array_map(static fn (array $row) : array => [$row['uri']], $rows));
        }

        return Command::SUCCESS;
    }
}
