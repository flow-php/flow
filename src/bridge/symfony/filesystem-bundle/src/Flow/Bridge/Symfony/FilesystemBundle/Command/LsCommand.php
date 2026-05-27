<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use DateTimeImmutable;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SizeUnits;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Throwable;

use function count;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function in_array;
use function json_encode;
use function ltrim;
use function rtrim;
use function sprintf;
use function str_ends_with;

#[AsCommand(
    name: 'flow:filesystem:ls',
    description: 'List files under a URI on a configured fstab.',
    aliases: ['flow:fs:ls'],
)]
final class LsCommand extends Command
{
    public const int DEFAULT_PAGE_SIZE = 10;

    public function __construct(
        private readonly FstabResolver $resolver,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'Directory URI, e.g. memory://data or file:///tmp')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->addOption('recursive', 'r', InputOption::VALUE_NONE, 'Recurse into subdirectories.')
            ->addOption('short', 's', InputOption::VALUE_NONE, 'Show only URIs (skip the type/size/modified columns).')
            ->addOption('limit', null, InputOption::VALUE_REQUIRED, 'Cap total entries at N. Default: unlimited.')
            ->addOption('offset', null, InputOption::VALUE_REQUIRED, 'Skip the first N entries before listing.', '0')
            ->addOption(
                'page-size',
                null,
                InputOption::VALUE_REQUIRED,
                'Entries per table page. Default: ' . self::DEFAULT_PAGE_SIZE . '.',
                (string) self::DEFAULT_PAGE_SIZE,
            )
            ->addOption(
                'format',
                null,
                InputOption::VALUE_REQUIRED,
                'Output format: "table" (default) or "json" (NDJSON, one JSON object per line).',
                'table',
            )
            ->setHelp(<<<'HELP'
                Lists entries under a directory URI on the chosen fstab.

                Paths must be full URIs in the form <protocol>://<path>. The default fstab is
                used unless --fstab is provided.

                By default the command lists all entries with type, size and modified
                columns, paginated in tables of --page-size rows (default 10). Use
                --short to drop everything except the URI column. On interactive terminals
                the command prompts between pages; piped/redirected output prints all
                pages continuously.

                Use --limit=N to cap total entries (handy for huge buckets when you just
                want a sample) and --offset=N to skip the first N entries. Offset is
                applied client-side: the backend still lists skipped entries (S3 has no
                native offset), so deep offsets on huge buckets can be slow.

                JSON format (--format=json) emits newline-delimited JSON (NDJSON) — one
                object per line — ignoring pagination.
                HELP);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));
            $recursive = type_boolean()->assert($input->getOption('recursive'));
            $long = !type_boolean()->assert($input->getOption('short'));
            // @mago-expect analysis:mixed-assignment
            $limitOpt = $input->getOption('limit');
            $limit = $limitOpt === null ? null : type_integer()->cast($limitOpt);
            $offset = type_integer()->cast($input->getOption('offset'));
            $pageSize = type_integer()->cast($input->getOption('page-size'));
            $format = type_string()->assert($input->getOption('format'));

            if (!in_array($format, ['table', 'json'], true)) {
                $io->getErrorStyle()->error(sprintf('Unsupported --format "%s". Use "table" or "json".', $format));

                return Command::FAILURE;
            }

            if ($limit !== null && $limit < 1) {
                $io->getErrorStyle()->error('--limit must be a positive integer.');

                return Command::FAILURE;
            }

            if ($offset < 0) {
                $io->getErrorStyle()->error('--offset must be a non-negative integer.');

                return Command::FAILURE;
            }

            if ($pageSize < 1) {
                $io->getErrorStyle()->error('--page-size must be a positive integer.');

                return Command::FAILURE;
            }

            $table = $this->resolver->resolve($fstabName);
            $userPath = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($userPath);

            $listPath = $this->buildListPath($userPath, $recursive);
        } catch (Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $iterator = $filesystem->list($listPath, new KeepAll());

        return $format === 'json'
            ? $this->renderNdjson($output, $iterator, $offset, $limit)
            : $this->renderTables($io, $input, $iterator, $offset, $limit, $pageSize, $long);
    }

    private function buildListPath(Path $userPath, bool $recursive): Path
    {
        if ($userPath->isPattern()) {
            return $userPath;
        }

        $uri = $userPath->uri();
        $suffix = $recursive ? '/**/*' : '/*';

        if (str_ends_with($uri, '://')) {
            return Path::from($uri . ltrim($suffix, '/'));
        }

        return Path::from(rtrim($uri, '/') . $suffix);
    }

    /**
     * @param iterable<FileStatus> $iterator
     */
    private function renderNdjson(OutputInterface $output, iterable $iterator, int $offset, ?int $limit): int
    {
        $skipped = 0;
        $count = 0;

        foreach ($iterator as $status) {
            if ($skipped < $offset) {
                $skipped++;

                continue;
            }

            if ($limit !== null && $count >= $limit) {
                break;
            }

            $output->writeln(json_encode([
                'uri' => $status->path->uri(),
                'type' => $status->isFile() ? 'file' : 'directory',
                'size' => $status->size,
                'modified' => $status->lastModifiedAt?->format(DateTimeImmutable::ATOM),
            ], JSON_THROW_ON_ERROR));

            $count++;
        }

        return Command::SUCCESS;
    }

    /**
     * @param iterable<FileStatus> $iterator
     * @param int<1, max> $pageSize
     */
    private function renderTables(
        SymfonyStyle $io,
        InputInterface $input,
        iterable $iterator,
        int $offset,
        ?int $limit,
        int $pageSize,
        bool $long,
    ): int {
        $headers = $long ? ['Type', 'Size', 'Modified', 'URI'] : ['URI'];

        $skipped = 0;
        $rendered = 0;
        $page = [];
        $pageCount = 0;
        $hasMore = false;
        $interactive = $input->isInteractive();

        foreach ($iterator as $status) {
            if ($skipped < $offset) {
                $skipped++;

                continue;
            }

            if ($limit !== null && $rendered >= $limit) {
                $hasMore = true;

                break;
            }

            $page[] = $long
                ? [
                    $status->isFile() ? 'file' : 'directory',
                    SizeUnits::humanReadable($status->size),
                    $status->lastModifiedAt?->format(DateTimeImmutable::ATOM) ?? '-',
                    $status->path->uri(),
                ]
                : [$status->path->uri()];

            $rendered++;

            if (count($page) === $pageSize) {
                $pageCount++;
                $io->table($headers, $page);
                $page = [];

                $limitReached = $limit !== null && $rendered >= $limit;

                if (
                    !$limitReached
                    && $interactive
                    && !$io->confirm(sprintf('Show next %d entries?', $pageSize), true)
                ) {
                    return Command::SUCCESS;
                }
            }
        }

        if ($page !== []) {
            $pageCount++;
            $io->table($headers, $page);
        }

        if ($hasMore) {
            $io->getErrorStyle()->warning(sprintf('Output truncated at %d entries. Raise with --limit=N.', $limit));
        }

        if ($pageCount === 0) {
            $io->writeln('<comment>No entries.</comment>');
        }

        return Command::SUCCESS;
    }
}
