<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

use function array_column;
use function array_filter;
use function array_values;
use function Flow\Types\DSL\type_string;

final class GenerateCodeCompletionCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully(): void
    {
        $commandTester = $this->executeCommand('app:generate:code-completion');

        static::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        static::assertStringContainsString('Generated code completion', $commandTester->getDisplay());
    }

    public function test_functions_carry_namespace_and_resolved_return_classes(): void
    {
        $this->executeCommand('app:generate:code-completion');

        $df = array_values(array_filter(
            $this->exportedFunctions(),
            static fn(array $function): bool => $function['name'] === 'df' && $function['namespace'] === 'Flow\ETL\DSL',
        ));

        static::assertCount(1, $df);
        static::assertSame(['Flow\ETL\Flow'], $df[0]['returns']);
    }

    public function test_functions_link_to_their_documentation_page(): void
    {
        $this->executeCommand('app:generate:code-completion');

        $toOutput = array_values(array_filter(
            $this->exportedFunctions(),
            static fn(array $function): bool => $function['name'] === 'to_output',
        ));

        static::assertSame('/documentation/dsl/core/to-output#dsl-function', $toOutput[0]['documentation']);
    }

    public function test_classes_carry_supertypes_and_own_methods(): void
    {
        $this->executeCommand('app:generate:code-completion');

        $classes = $this->exportedClasses();

        static::assertContains(
            'Flow\ETL\Functions\ScalarFunctionChain',
            $classes['Flow\ETL\Functions\Reference\UnresolvedReference']['supertypes'],
        );
        static::assertContains('isNull', array_column(
            $classes['Flow\ETL\Functions\ScalarFunctionChain']['methods'],
            'name',
        ));
    }

    public function test_classes_link_to_their_api_documentation_page(): void
    {
        $this->executeCommand('app:generate:code-completion');

        static::assertSame(
            '/documentation/api/core/classes/Flow-ETL-API-DataFrame.html',
            $this->exportedClasses()['Flow\ETL\API\DataFrame']['api'] ?? null,
        );
    }

    public function test_classes_carry_static_methods_and_constants(): void
    {
        $this->executeCommand('app:generate:code-completion');

        $classes = $this->exportedClasses();

        static::assertContains('Overwrite', $classes['Flow\ETL\IO\Filesystem\SaveMode']['constants']);
        static::assertContains('tree', $classes['Flow\ETL\Planning\Logical\Format']['constants']);
        static::assertContains('fromArray', array_column($classes['Flow\ETL\Values\Schema']['staticMethods'], 'name'));
    }

    public function test_doc_comments_are_decoded_plain_text(): void
    {
        $this->executeCommand('app:generate:code-completion');

        $batchSize = array_values(array_filter(
            $this->exportedClasses()['Flow\ETL\API\DataFrame']['methods'],
            static fn(array $method): bool => $method['name'] === 'batchSize',
        ));

        static::assertStringStartsWith(
            'Merge/Split Rows yielded by Extractor',
            type_string()->assert($batchSize[0]['doc']),
        );
        static::assertStringNotContainsString('/**', type_string()->assert($batchSize[0]['doc']));
    }
}
