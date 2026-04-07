<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_null, type_string, type_union};
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:stat', description: 'Print metadata about a file or directory URI.', aliases: ['flow:fs:stat'])]
final class StatCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    protected function configure() : void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'File or directory URI')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->addOption('format', null, InputOption::VALUE_REQUIRED, 'Output format: "human" (default) or "json".', 'human')
            ->setHelp('Prints metadata (URI, protocol, type, size) for the given URI.');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));
            $format = type_string()->assert($input->getOption('format'));

            if (!\in_array($format, ['human', 'json'], true)) {
                $io->getErrorStyle()->error(\sprintf('Unsupported --format "%s". Use "human" or "json".', $format));

                return Command::FAILURE;
            }

            $table = $this->resolver->resolve($fstabName);
            $path = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($path);

            $status = $filesystem->status($path);

            if ($status === null) {
                $io->getErrorStyle()->error(\sprintf('Path not found: %s', $path->uri()));

                return Command::FAILURE;
            }

            $type = $status->isFile() ? 'file' : 'directory';
            $size = $status->isFile() ? $filesystem->readFrom($status->path)->size() : null;
            $protocolName = $status->path->protocol()->name;
            $uri = $status->path->uri();
            $cleanPath = $status->path->path();
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        if ($format === 'json') {
            $output->writeln((string) \json_encode([
                'uri' => $uri,
                'protocol' => $protocolName,
                'path' => $cleanPath,
                'type' => $type,
                'size' => $size,
            ]));

            return Command::SUCCESS;
        }

        $io->definitionList(
            ['URI' => $uri],
            ['Protocol' => $protocolName],
            ['Path' => $cleanPath],
            ['Type' => $type],
            ['Size' => $size === null ? '-' : (string) $size],
        );

        return Command::SUCCESS;
    }
}
