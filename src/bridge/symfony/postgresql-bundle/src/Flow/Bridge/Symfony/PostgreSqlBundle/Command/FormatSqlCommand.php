<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};
use function Flow\PostgreSql\DSL\sql_format;
use function Flow\Types\DSL\type_string;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path as FsPath;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'flow:sql:format', description: 'Format SQL using the flow-php/postgresql parser. Accepts a raw SQL string or a --path with glob support.')]
final class FormatSqlCommand extends Command
{
    private readonly NativeLocalFilesystem $filesystem;

    public function __construct()
    {
        parent::__construct();
        $this->filesystem = native_local_filesystem();
    }

    protected function configure() : void
    {
        $this
            ->addArgument('sql', InputArgument::OPTIONAL, 'Raw SQL string to format. Ignored when --path is used.')
            ->addOption('path', 'p', InputOption::VALUE_REQUIRED, 'Path, directory or glob pattern (only files with .sql extension are processed)')
            ->addOption('write', 'w', InputOption::VALUE_NONE, 'Write formatted SQL back to source files instead of printing to stdout')
            ->addOption('check', null, InputOption::VALUE_NONE, 'Exit with non-zero status when any input is not already formatted (does not modify files)');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $pathOption = $input->getOption('path');
        $sqlArgument = $input->getArgument('sql');
        $write = (bool) $input->getOption('write');
        $check = (bool) $input->getOption('check');

        if ($pathOption === null && $sqlArgument === null) {
            $output->writeln('<error>Either a SQL string argument or --path option must be provided.</error>');

            return Command::FAILURE;
        }

        if ($pathOption !== null) {
            return $this->formatPath(type_string()->assert($pathOption), $write, $check, $output);
        }

        $sql = type_string()->assert($sqlArgument);
        $formatted = sql_format($sql);

        if ($check) {
            if (\trim($formatted) !== \trim($sql)) {
                $output->writeln('<error>SQL is not formatted.</error>');

                return Command::FAILURE;
            }

            $output->writeln('<info>SQL is already formatted.</info>');

            return Command::SUCCESS;
        }

        $output->writeln($formatted);

        return Command::SUCCESS;
    }

    /**
     * @return list<FsPath>
     */
    private function collectSqlFiles(FsPath $path) : array
    {
        $status = $this->filesystem->status($path);

        if ($status !== null && $status->isDirectory()) {
            $path = path(\rtrim($path->path(), '/') . '/**/*.sql', $path->options());
        }

        $files = [];

        foreach ($this->filesystem->list($path) as $file) {
            if (!$file->isFile()) {
                continue;
            }

            if ($file->path->extension() !== 'sql') {
                continue;
            }

            $files[] = $file->path;
        }

        return $files;
    }

    private function formatPath(string $pathString, bool $write, bool $check, OutputInterface $output) : int
    {
        $files = $this->collectSqlFiles(path($pathString));

        if ($files === []) {
            $output->writeln(\sprintf('<comment>No .sql files found at: %s</comment>', $pathString));

            return Command::SUCCESS;
        }

        $unformatted = 0;
        $formattedCount = 0;

        foreach ($files as $file) {
            $original = $this->filesystem->readFrom($file)->content();

            try {
                $formatted = sql_format($original);
            } catch (\Throwable $e) {
                $output->writeln(\sprintf('<error>Failed to format %s: %s</error>', $file->path(), $e->getMessage()));

                return Command::FAILURE;
            }

            $isDifferent = \trim($formatted) !== \trim($original);

            if ($check) {
                if ($isDifferent) {
                    $unformatted++;
                    $output->writeln(\sprintf('<error>Not formatted: %s</error>', $file->path()));
                }

                continue;
            }

            if ($write) {
                if ($isDifferent) {
                    $this->filesystem->writeTo($file)->append($formatted)->close();
                    $formattedCount++;
                    $output->writeln(\sprintf('<info>Formatted: %s</info>', $file->path()));
                }

                continue;
            }

            $output->writeln(\sprintf('<comment>-- %s</comment>', $file->path()));
            $output->writeln($formatted);
            $output->writeln('');
        }

        if ($check) {
            if ($unformatted > 0) {
                $output->writeln(\sprintf('<error>%d file(s) not formatted.</error>', $unformatted));

                return Command::FAILURE;
            }

            $output->writeln(\sprintf('<info>All %d file(s) are properly formatted.</info>', \count($files)));

            return Command::SUCCESS;
        }

        if ($write) {
            $output->writeln(\sprintf('<info>%d file(s) reformatted, %d unchanged.</info>', $formattedCount, \count($files) - $formattedCount));
        }

        return Command::SUCCESS;
    }
}
