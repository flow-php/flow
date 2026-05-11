<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Flow\Website\Model\Documentation\DSLDefinition;
use Flow\Website\Service\Documentation\DSLDefinitions;
use Flow\Website\Service\FlowConfigFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

use function Flow\Filesystem\DSL\path;

#[AsCommand(name: 'app:generate:dsl-completer', description: 'Generate CodeMirror completer for all DSL functions')]
final class GenerateDSLCompleterCommand extends Command
{
    public function __construct(
        private readonly Environment $twig,
        private readonly DSLDefinitions $dslDefinitions,
        private readonly string $projectDir,
        private readonly FlowConfigFactory $configFactory,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Generating DSL Functions Completer');

        $dslFunctions = \array_filter(
            $this->dslDefinitions->all(),
            static fn(DSLDefinition $definition): bool => $definition->type() !== null,
        );

        $io->info(\sprintf('Found %d DSL functions', \count($dslFunctions)));

        $functionsData = \array_map(fn(DSLDefinition $definition): array => [
            'name' => $definition->name(),
            'fullName' => '\\' . $definition->data()['namespace'] . '\\' . $definition->name(),
            'doc_comment' => $definition->data()['doc_comment'],
            'meta' => 'flow-dsl-' . $this->getTypeName($definition),
            'parameters' => $definition->data()['parameters'],
            'return_type' => $definition->data()['return_type'] ?? [],
        ], $dslFunctions);

        $content = $this->twig->render('completers/dsl-codemirror.js.twig', [
            'functions' => $functionsData,
            'generated_at' => new \DateTime(),
            'total_count' => \count($functionsData),
        ]);

        $outputFile = $this->projectDir . '/assets/codemirror/completions/dsl.js';

        $this->configFactory->filesystem()->writeTo(path($outputFile))->append($content)->close();

        $io->success("Generated DSL completer: {$outputFile}");
        $io->info('Functions included: ' . \count($functionsData));

        return Command::SUCCESS;
    }

    private function getTypeName(DSLDefinition $definition): string
    {
        $type = $definition->type();

        return $type ? \strtolower(\str_replace(' ', '-', $type->value)) : 'unknown';
    }
}
