<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Throwable;

use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function sprintf;

#[AsCommand(name: 'flow:filesystem:cat', description: 'Stream a file URI to STDOUT.', aliases: ['flow:fs:cat'])]
final class CatCommand extends Command
{
    public function __construct(
        private readonly FstabResolver $resolver,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'File URI, e.g. memory://hello.txt')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->setHelp('Stream the contents of a file URI to STDOUT. Refuses on directories.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));

            $table = $this->resolver->resolve($fstabName);
            $path = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($path);

            $status = $filesystem->status($path);

            if ($status === null) {
                $io->getErrorStyle()->error(sprintf('File not found: %s', $path->uri()));

                return Command::FAILURE;
            }

            if ($status->isDirectory()) {
                $io->getErrorStyle()->error(sprintf('Refusing to cat directory: %s', $path->uri()));

                return Command::FAILURE;
            }

            $stream = $filesystem->readFrom($path);

            foreach ($stream->iterate(8192) as $chunk) {
                $output->write($chunk, false, OutputInterface::OUTPUT_RAW);
            }

            $stream->close();
        } catch (Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
}
