<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Model\Documentation\Module;
use Flow\Website\Service\Documentation\DSLDefinitions;
use Flow\Website\Service\Examples;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DocumentationController extends AbstractController
{
    public function __construct(
        private readonly DSLDefinitions $dslDefinitions,
        private readonly Examples $examples,
    ) {
    }

    #[Route('/documentation/dsl', name: 'documentation', options: ['sitemap' => true])]
    public function dsl() : Response
    {
        $modules = $this->dslDefinitions->modules();

        return $this->render('documentation/dsl.html.twig', [
            'module_name' => $module = 'core',
            'modules' => $modules,
            'definitions' => $this->dslDefinitions->fromModule(Module::fromName($module)),
            'types' => $this->dslDefinitions->types(),
        ]);
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
}
