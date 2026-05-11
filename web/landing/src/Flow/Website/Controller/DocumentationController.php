<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Model\Documentation\Module;
use Flow\Website\Model\Documentation\Page;
use Flow\Website\Service\Documentation\DSLDefinitions;
use Flow\Website\Service\Documentation\Pages;
use Flow\Website\Service\Examples;
use Flow\Website\Service\Manifest\PackageMeta;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DocumentationController extends AbstractController
{
    public function __construct(
        private readonly Pages $pages,
        private readonly DSLDefinitions $dslDefinitions,
        private readonly Examples $examples,
        private readonly PackageMeta $packageMeta,
    ) {}

    #[Route('/documentation/api/{page}', name: 'documentation_api', requirements: ['page' => '.*'])]
    public function apiPage(string $page): Response
    {
        $projectDir = $this->getParameter('kernel.project_dir');
        $docsDir = $projectDir . '/build/documentation/api';

        if (!\file_exists($docsDir . '/' . $page) || !\is_file($docsDir . '/' . $page)) {
            if (!\str_ends_with($page, '.html')) {
                return $this->redirectToRoute('documentation_api', ['page' => $page . '/index.html']);
            }
        }

        if (\file_exists($docsDir . '/' . $page)) {
            $extension = pathinfo($docsDir . '/' . $page, PATHINFO_EXTENSION);

            $contentType = match ($extension) {
                'css' => 'text/css',
                'js' => 'application/javascript',
                default => 'text/html',
            };

            $body = \file_get_contents($docsDir . '/' . $page);

            if ($extension === 'html' && $body !== false) {
                $body = \str_replace(
                    '</head>',
                    '<link rel="stylesheet" href="/styles/api-overrides.css"></head>',
                    $body,
                );
            }

            return new Response($body, 200, [
                'Content-Type' => $contentType,
            ]);
        }

        throw $this->createNotFoundException();
    }

    #[Route('/documentation/dsl/{module}/{function}', name: 'documentation_dsl_function')]
    public function dslFunction(string $module, string $function): Response
    {
        $modules = $this->dslDefinitions->modules();

        $definition = $this->dslDefinitions->fromModule(Module::fromName($module))->get($function);
        $examples = [];

        foreach ($definition->examples() as $example) {
            $examples[] = [
                'code' => $this->examples->code($example->topic, $example->name, $example->option),
                'topic' => $example->topic,
                'name' => $example->name,
                'option' => $example->option,
            ];
        }

        return $this->render('documentation/dsl/function.html.twig', [
            'module_name' => $module,
            'modules' => $modules,
            'definition' => $definition,
            'examples' => $examples,
            'types' => $this->dslDefinitions->types(),
            'searchFacets' => $this->dslFacets($module),
        ]);
    }

    #[Route('/documentation/dsl/{module}', name: 'documentation_dsl')]
    public function dslModule(string $module = 'core'): Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/dsl.html.twig', [
            'module_name' => $module,
            'modules' => $modules,
            'definitions' => $this->dslDefinitions->fromModule(Module::fromName($module)),
            'types' => $this->dslDefinitions->types(),
            'searchFacets' => $this->dslFacets($module),
        ]);
    }

    #[Route('/documentation/example/{topic}/{example}/', name: 'documentation_example', priority: -90)]
    public function example(string $topic, string $example): Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = $example;

        return $this->render('documentation/example.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $topic,
            'currentExample' => $example,
            'description' => $this->examples->description($currentTopic, $currentExample),
            'documentation' => $this->examples->documentation($currentTopic, $currentExample),
            'code' => $this->examples->code($currentTopic, $currentExample),
            'searchFacets' => ['type' => 'Example'],
        ]);
    }

    #[Route('/documentation', name: 'documentation', options: ['sitemap' => true])]
    public function index(): Response
    {
        $page = $this->pages->get('introduction.md');

        return $this->render('documentation/page.html.twig', [
            'page' => $page,
            'searchFacets' => $this->pageFacets($page),
        ]);
    }

    public function navigationLeft(string $currentPath = ''): Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/navigation_left.html.twig', [
            'examples' => $this->examples,
            'modules' => $modules,
            'types' => $this->dslDefinitions->types(),
            'currentPath' => $currentPath,
        ]);
    }

    public function navigationRight(string $currentPath = ''): Response
    {
        return $this->render('documentation/navigation_right.html.twig', [
            'currentPath' => $currentPath,
        ]);
    }

    #[Route('/documentation/{path}', name: 'documentation_page', requirements: ['path' => '.*'], priority: -100)]
    public function page(string $path): Response
    {
        $page = $this->pages->get($path);

        return $this->render('documentation/page.html.twig', [
            'page' => $page,
            'searchFacets' => $this->pageFacets($page),
        ]);
    }

    /**
     * @return array{type?: string, component?: string}
     */
    private function dslFacets(string $module): array
    {
        $facets = ['type' => 'DSL'];

        $component = $this->packageMeta->forDslModule($module);

        if ($component !== null) {
            $facets['component'] = $component;
        }

        return $facets;
    }

    /**
     * @return array{type?: string, component?: string}
     */
    private function pageFacets(Page $page): array
    {
        $packageName = $page->package();

        if ($packageName === null) {
            return [];
        }

        $meta = $this->packageMeta->forPackage($packageName);

        if ($meta === null) {
            return [];
        }

        return $meta;
    }
}
