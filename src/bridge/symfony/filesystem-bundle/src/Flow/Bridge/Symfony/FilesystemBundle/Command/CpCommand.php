<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_null, type_string, type_union};
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Filesystem\{FilesystemTable, Path};
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:cp', description: 'Copy a file between two URIs on the same fstab.', aliases: ['flow:fs:cp'])]
final class CpCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    public static function streamCopy(FilesystemTable $table, Path $source, Path $dest, string $fstabName) : void
    {
        try {
            $sourceFs = $table->for($source);
        } catch (\Throwable $e) {
            throw new InvalidArgumentException(\sprintf('in fstab "%s": source: %s', $fstabName, $e->getMessage()), 0, $e);
        }

        try {
            $destFs = $table->for($dest);
        } catch (\Throwable $e) {
            throw new InvalidArgumentException(\sprintf('in fstab "%s": destination: %s', $fstabName, $e->getMessage()), 0, $e);
        }

        $sourceStatus = $sourceFs->status($source);

        if ($sourceStatus === null) {
            throw new InvalidArgumentException(\sprintf('Source not found: %s', $source->uri()));
        }

        if ($sourceStatus->isDirectory()) {
            throw new InvalidArgumentException(\sprintf('Refusing to copy directory: %s', $source->uri()));
        }

        $sourceStream = $sourceFs->readFrom($source);
        $destStream = $destFs->writeTo($dest);

        try {
            foreach ($sourceStream->iterate(8192) as $chunk) {
                $destStream->append($chunk);
            }
        } finally {
            $destStream->close();
            $sourceStream->close();
        }
    }

    protected function configure() : void
    {
        $this
            ->addArgument('source', InputArgument::REQUIRED, 'Source file URI')
            ->addArgument('destination', InputArgument::REQUIRED, 'Destination file URI')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->setHelp(<<<'HELP'
                Copies a file from one URI to another. Both URIs MUST resolve through the same
                fstab; cross-fstab transfers are not supported. Streams in 8 KiB chunks so
                large files do not blow up memory.
                HELP);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawSource = type_string()->assert($input->getArgument('source'));
            $rawDest = type_string()->assert($input->getArgument('destination'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));

            $table = $this->resolver->resolve($fstabName);
            $source = $this->resolver->parseUri($rawSource);
            $dest = $this->resolver->parseUri($rawDest);

            self::streamCopy($table, $source, $dest, $fstabName ?? $this->resolver->defaultFstabName());
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $io->success(\sprintf('Copied %s → %s', $source->uri(), $dest->uri()));

        return Command::SUCCESS;
    }
}
