<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Model\Documentation\Module;
use Flow\Website\Service\Documentation\{DSLDefinitions, Pages};
use Flow\Website\Service\Examples;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DocumentationController extends AbstractController
{
    public function __construct(
        private readonly Pages $pages,
        private readonly DSLDefinitions $dslDefinitions,
        private readonly Examples $examples,
    ) {
    }

    #[Route('/documentation/dsl/{module}/{function}', name: 'documentation_dsl_function')]
    public function dslFunction(string $module, string $function) : Response
    {
        $modules = $this->dslDefinitions->modules();

        $definition = $this->dslDefinitions->fromModule(Module::fromName($module))->get($function);
        $examples = [];

        foreach ($definition->examples() as $example) {
            $examples[] = [
                'code' => $this->examples->code($example->topic, $example->name),
                'topic' => $example->topic,
                'name' => $example->name,
            ];
        }

        return $this->render('documentation/dsl/function.html.twig', [
            'module_name' => $module,
            'modules' => $modules,
            'definition' => $definition,
            'examples' => $examples,
            'types' => $this->dslDefinitions->types(),
        ]);
    }

    #[Route('/documentation/dsl/{module}', name: 'documentation_dsl')]
    public function dslModule(string $module = 'core') : Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/dsl.html.twig', [
            'module_name' => $module,
            'modules' => $modules,
            'definitions' => $this->dslDefinitions->fromModule(Module::fromName($module)),
            'types' => $this->dslDefinitions->types(),
        ]);
    }

    #[Route('/documentation/example/{topic}/{example}/', name: 'documentation_example', priority: -90)]
    public function example(string $topic, string $example) : Response
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
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }

    public function examples() : Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/examples.html.twig', [
            'examples' => $this->examples,
            'modules' => $modules,
            'types' => $this->dslDefinitions->types(),
        ]);
    }

    #[Route('/documentation', name: 'documentation', options: ['sitemap' => true])]
    public function index() : Response
    {
        return $this->render('documentation/page.html.twig', [
            'page' => $this->pages->get('introduction.md'),
        ]);
    }

    public function navigation() : Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/navigation.html.twig', [
            'examples' => $this->examples,
            'modules' => $modules,
            'types' => $this->dslDefinitions->types(),
        ]);
    }

    #[Route('/documentation/{path}', name: 'documentation_page', requirements: ['path' => '.*'], priority: -100)]
    public function page(string $path) : Response
    {
        return $this->render('documentation/page.html.twig', [
            'page' => $this->pages->get($path),
        ]);
    }
}
