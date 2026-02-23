<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{df, lit, ref};
use function Flow\Filesystem\DSL\path;
use Flow\Website\Service\FlowConfigFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

#[AsCommand(
    name: 'app:generate:scalar-function-chain-completer',
    description: 'Generate CodeMirror completer for ScalarFunctionChain methods'
)]
final class GenerateScalarFunctionChainCompleterCommand extends Command
{
    public function __construct(
        private readonly Environment $twig,
        private readonly string $projectDir,
        private readonly FlowConfigFactory $configFactory,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Generating ScalarFunctionChain Methods Completer');

        $apiJsonPath = $this->projectDir . '/../../web/landing/resources/api.json';
        $dslJsonPath = $this->projectDir . '/../../web/landing/resources/dsl.json';

        $fs = $this->configFactory->filesystem();

        if ($fs->status(path($apiJsonPath)) === null) {
            $io->error("API JSON file not found: {$apiJsonPath}");

            return Command::FAILURE;
        }

        if ($fs->status(path($dslJsonPath)) === null) {
            $io->error("DSL JSON file not found: {$dslJsonPath}");

            return Command::FAILURE;
        }

        $scalarFunctionChainFunctions = df($this->configFactory->configBuilder('scalar_function_chain_completer'))
            ->read(from_json($dslJsonPath))
            ->collect()
            ->filter(ref('scalar_function_chain')->equals(lit(true)))
            ->fetch()
            ->reduceToArray('name');

        $io->info(\sprintf('Found %d DSL functions with scalar_function_chain flag', \count($scalarFunctionChainFunctions)));

        $methodsData = df($this->configFactory->configBuilder('scalar_function_chain_completer'))
            ->read(from_json($apiJsonPath))
            ->collect()
            ->filter(ref('class_slug')->equals(lit('scalarfunctionchain')))
            ->select('name', 'class', 'class_slug', 'parameters', 'return_type', 'doc_comment')
            ->fetch()
            ->toArray();

        $io->info(\sprintf('Found %d ScalarFunctionChain methods', \count($methodsData)));

        $content = $this->twig->render('completers/scalarfunctionchain-codemirror.js.twig', [
            'scalarfunctionchain_methods' => $methodsData,
            'scalarfunctionchain_functions' => $scalarFunctionChainFunctions,
            'generated_at' => new \DateTime(),
            'total_count' => \count($methodsData),
        ]);

        $outputFile = $this->projectDir . '/assets/codemirror/completions/scalarfunctionchain.js';

        $fs->writeTo(path($outputFile))->append($content)->close();

        $io->success("Generated ScalarFunctionChain completer: {$outputFile}");
        $io->info('ScalarFunctionChain methods: ' . \count($methodsData));
        $io->info('ScalarFunctionChain functions: ' . \count($scalarFunctionChainFunctions));

        return Command::SUCCESS;
    }
}
