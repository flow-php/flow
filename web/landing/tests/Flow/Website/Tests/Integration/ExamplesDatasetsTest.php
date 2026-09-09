<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function file_get_contents;
use function glob;
use function preg_match_all;
use function sprintf;
use function str_replace;

final class ExamplesDatasetsTest extends TestCase
{
    public function test_every_example_references_a_shipped_and_registered_dataset(): void
    {
        $landingDir = __DIR__ . '/../../../../..';
        // references and map keys are both rooted at "data/", which lives under assets/wasm/
        $wasmDir = $landingDir . '/assets/wasm';

        $registered = [];
        preg_match_all(
            "#'(data/[^']+)': asset\('wasm/(data/[^']+)'\)#",
            (string) file_get_contents($landingDir . '/templates/playground/_playground.html.twig'),
            $registered,
        );

        $checked = 0;

        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
            $landingDir . '/content/examples',
            RecursiveDirectoryIterator::SKIP_DOTS,
        )) as $file) {
            /** @var SplFileInfo $file */
            if ($file->getFilename() !== 'code.php') {
                continue;
            }

            $references = [];
            preg_match_all(
                "#__DIR__ \. '/(data/[^']+)'#",
                (string) file_get_contents($file->getPathname()),
                $references,
            );

            foreach ($references[1] as $reference) {
                $matches = glob($wasmDir . '/' . $reference) ?: [];

                static::assertNotEmpty($matches, sprintf(
                    '%s references "%s" which the playground does not ship',
                    $file->getPathname(),
                    $reference,
                ));

                foreach ($matches as $match) {
                    static::assertContains(
                        str_replace($wasmDir . '/', '', $match),
                        $registered[1],
                        sprintf(
                            '%s references "%s" which is not registered in the wasm_resources map',
                            $file->getPathname(),
                            $reference,
                        ),
                    );
                }

                $checked++;
            }
        }

        static::assertGreaterThan(0, $checked, 'No dataset references were found to check');
    }
}
