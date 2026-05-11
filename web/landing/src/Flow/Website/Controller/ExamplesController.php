<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Examples;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class ExamplesController extends AbstractController
{
    public function __construct(
        private readonly Examples $examples,
    ) {}

    #[Route(
        '/playground/{topic}/{example}/data/{path}',
        name: 'example_data',
        requirements: ['path' => '.+'],
        priority: 15,
    )]
    public function data(string $topic, string $example, string $path): Response
    {
        $filePath = $this->examples->dataFilePath($topic, $example, $path);

        if (null === $filePath || !\file_exists($filePath)) {
            throw $this->createNotFoundException();
        }

        return new BinaryFileResponse($filePath);
    }

    #[Route('/{topic}/{example}/', name: 'example', priority: -140)]
    public function example(string $topic, string $example): Response
    {
        $firstOption = $this->examples->firstOption($topic, $example);

        return $this->render('example/index.html.twig', [
            'topicsNavigation' => $this->examples->topicsNavigation(),
            'examplesNavigation' => $this->examples->examplesNavigation($topic),
            'optionsNavigation' => $this->examples->optionsNavigation($topic, $example),
            'currentTopic' => $topic,
            'currentExample' => $example,
            'currentOption' => null,
            'description' => $this->examples->description($topic, $example, $firstOption),
            'documentation' => $this->examples->documentation($topic, $example, $firstOption),
            'code' => $this->examples->code($topic, $example, $firstOption),
            'playgroundUrl' => $this->examples->playgroundUrl($topic, $example, $firstOption),
        ]);
    }

    #[Route('/playground/{topic}/{example}/{option}', name: 'example_option_playground', priority: 10)]
    public function exampleOptionPlayground(string $topic, string $example, string $option): Response
    {
        return $this->render('example/playground.html.twig', [
            'topic' => $topic,
            'example' => $example,
            'option' => $option,
            'code' => $this->examples->code($topic, $example, $option),
            'data_files' => $this->examples->dataFiles($topic, $example, $option),
            'backUrl' => $this->generateUrl('example_option', [
                'topic' => $topic,
                'example' => $example,
                'option' => $option,
            ]),
        ]);
    }

    #[Route('/playground/{topic}/{example}', name: 'example_playground', priority: 5)]
    public function examplePlayground(string $topic, string $example): Response
    {
        $firstOption = $this->examples->firstOption($topic, $example);

        return $this->render('example/playground.html.twig', [
            'topic' => $topic,
            'example' => $example,
            'option' => $firstOption,
            'code' => $this->examples->code($topic, $example, $firstOption),
            'data_files' => $this->examples->dataFiles($topic, $example, $firstOption),
            'backUrl' => $this->generateUrl($firstOption ? 'example_option' : 'example', \array_filter([
                'topic' => $topic,
                'example' => $example,
                'option' => $firstOption,
            ])),
        ]);
    }

    #[Route('/{topic}/{example}/{option}/', name: 'example_option', priority: -150)]
    public function option(string $topic, string $example, string $option): Response
    {
        return $this->render('example/index.html.twig', [
            'topicsNavigation' => $this->examples->topicsNavigation(),
            'examplesNavigation' => $this->examples->examplesNavigation($topic),
            'optionsNavigation' => $this->examples->optionsNavigation($topic, $example),
            'currentTopic' => $topic,
            'currentExample' => $example,
            'currentOption' => $option,
            'description' => $this->examples->description($topic, $example, $option),
            'documentation' => $this->examples->documentation($topic, $example, $option),
            'code' => $this->examples->code($topic, $example, $option),
            'playgroundUrl' => $this->examples->playgroundUrl($topic, $example, $option),
        ]);
    }

    #[Route(
        '/playground/{topic}/{example}/{option}/data/{path}',
        name: 'example_option_data',
        requirements: ['path' => '.+'],
        priority: 20,
    )]
    public function optionData(string $topic, string $example, string $option, string $path): Response
    {
        $filePath = $this->examples->dataFilePath($topic, $example, $path, $option);

        if (null === $filePath || !\file_exists($filePath)) {
            throw $this->createNotFoundException();
        }

        return new BinaryFileResponse($filePath);
    }

    #[Route('/{topic}/', name: 'topic', priority: -120)]
    public function topic(string $topic): Response
    {
        $firstExample = $this->examples->firstExample($topic);
        $firstOption = $this->examples->firstOption($topic, $firstExample);

        return $this->render('example/index.html.twig', [
            'topicsNavigation' => $this->examples->topicsNavigation(),
            'examplesNavigation' => $this->examples->examplesNavigation($topic),
            'optionsNavigation' => $this->examples->optionsNavigation($topic, $firstExample),
            'currentTopic' => $topic,
            'currentExample' => $firstExample,
            'currentOption' => $firstOption,
            'description' => $this->examples->description($topic, $firstExample, $firstOption),
            'documentation' => $this->examples->documentation($topic, $firstExample, $firstOption),
            'code' => $this->examples->code($topic, $firstExample, $firstOption),
            'playgroundUrl' => $this->examples->playgroundUrl($topic, $firstExample, $firstOption),
        ]);
    }
}
