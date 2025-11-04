<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Flow\Website\Model\Documentation\DSLDefinition;
use Flow\Website\Service\Documentation\DSLDefinitions;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

#[AsCommand(
    name: 'app:generate:dsl-completer',
    description: 'Generate ACE Editor completer for all DSL functions'
)]
final class GenerateDSLCompleterCommand extends Command
{
    public function __construct(
        private readonly Environment $twig,
        private readonly DSLDefinitions $dslDefinitions,
        private readonly string $projectDir,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Generating DSL Functions Completer');

        $dslFunctions = \array_filter(
            $this->dslDefinitions->all(),
            static fn (DSLDefinition $definition) : bool => $definition->type() !== null
        );

        $io->info(\sprintf('Found %d DSL functions', \count($dslFunctions)));

        $functionsData = \array_map(
            fn (DSLDefinition $definition) : array => [
                'name' => $definition->name(),
                'snippet' => $this->buildSnippet($definition),
                'docComment' => $this->formatDocComment($definition),
                'highlightedSignature' => $this->buildHighlightedSignature($definition),
                'meta' => 'flow-dsl-' . $this->getTypeName($definition),
            ],
            $dslFunctions
        );

        $content = $this->twig->render('completers/flow_dsl.js.twig', [
            'functions' => $functionsData,
            'generated_at' => new \DateTime(),
            'total_count' => \count($functionsData),
        ]);

        $outputFile = $this->projectDir . '/assets/ace/completers/flow_dsl.js';
        $outputDir = \dirname($outputFile);

        if (!\is_dir($outputDir)) {
            \mkdir($outputDir, 0755, true);
            $io->info("Created directory: {$outputDir}");
        }

        \file_put_contents($outputFile, $content);

        $io->success("Generated DSL completer: {$outputFile}");
        $io->info('Functions included: ' . \count($functionsData));

        return Command::SUCCESS;
    }

    /**
     * @param array{name: string, type?: array, has_default_value: bool} $param
     */
    private function buildHighlightedParam(array $param) : string
    {
        $paramStr = '';

        if (!empty($param['type'])) {
            $paramStr .= '<span class="fn-type">' . $this->formatType($param['type']) . '</span> ';
        }

        $paramStr .= '<span class="fn-param">$' . $param['name'] . '</span>';

        if ($param['has_default_value']) {
            $paramStr .= ' <span class="fn-operator">=</span> <span class="fn-operator">...</span>';
        }

        return $paramStr;
    }

    private function buildHighlightedSignature(DSLDefinition $definition) : string
    {
        $params = \array_map(
            fn (array $param) : string => $this->buildHighlightedParam($param),
            $definition->data()['parameters']
        );

        $signature = '<span class="fn-name">' . $definition->name() . '</span>'
            . '<span class="fn-operator">(</span>'
            . \implode('<span class="fn-operator">,</span> ', $params)
            . '<span class="fn-operator">)</span>';

        if (!empty($definition->data()['return_type'])) {
            $signature .= ' <span class="fn-operator">:</span> '
                . '<span class="fn-return">' . $this->formatType($definition->data()['return_type']) . '</span>';
        }

        return $signature;
    }

    private function buildSnippet(DSLDefinition $definition) : string
    {
        $params = $definition->data()['parameters'];

        if (empty($params)) {
            return $definition->name() . '()';
        }

        $snippetParams = [];
        $tabstop = 1;

        foreach ($params as $param) {
            $typeHint = !empty($param['type'])
                ? $this->formatType($param['type']) . ' '
                : '';

            $snippetParams[] = '${' . $tabstop . ':' . $typeHint . '$' . $param['name'] . '}';
            $tabstop++;
        }

        return $definition->name() . '(' . \implode(', ', $snippetParams) . ')';
    }

    private function formatDocComment(DSLDefinition $definition) : string
    {
        if (!$definition->hasDocComment()) {
            return '';
        }

        $docComment = \preg_replace('/^\/\*\*|\*\/$/', '', $definition->docComment());
        $lines = \explode("\n", (string) $docComment);
        $lines = \array_map(static fn (string $line) : string => \preg_replace('/^\s*\*\s?/', '', $line), $lines);
        $lines = \array_filter($lines, static fn (string $line) : bool => \trim($line) !== '');

        return \implode('<br>', $lines);
    }

    /**
     * @param array<array{name: string}> $types
     */
    private function formatType(array $types) : string
    {
        return \implode('|', \array_map(static fn (array $t) : string => $t['name'], $types));
    }

    private function getTypeName(DSLDefinition $definition) : string
    {
        $type = $definition->type();

        return $type ? \strtolower(\str_replace(' ', '-', $type->value)) : 'unknown';
    }
}
