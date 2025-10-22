<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Examples;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class ExamplesController extends AbstractController
{
    public function __construct(
        private readonly Examples $examples,
    ) {
    }

    #[Route('/{topic}/{example}/', name: 'example', priority: -140)]
    public function example(string $topic, string $example) : Response
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
            'composer' => $this->examples->composer($topic, $example, $firstOption),
            'code' => $this->examples->code($topic, $example, $firstOption),
            'output' => $this->examples->output($topic, $example, $firstOption),
        ]);
    }

    #[Route('/{topic}/{example}/{option}/', name: 'example_option', priority: -150)]
    public function option(string $topic, string $example, string $option) : Response
    {
        return $this->render('example/index.html.twig', [
            'topicsNavigation' => $this->examples->topicsNavigation(),
            'examplesNavigation' => $this->examples->examplesNavigation($topic),
            'optionsNavigation' => $this->examples->optionsNavigation($topic, $example),
            'currentTopic' => $topic,
            'currentExample' => $example,
            'currentOption' => $option,
            'description' => $this->examples->description($topic, $example, $option),
            'composer' => $this->examples->composer($topic, $example, $option),
            'code' => $this->examples->code($topic, $example, $option),
            'output' => $this->examples->output($topic, $example, $option),
        ]);
    }

    #[Route('/{topic}/', name: 'topic', priority: -120)]
    public function topic(string $topic) : Response
    {
        $topics = $this->examples->topics();
        $firstExample = $this->examples->firstExample($topic);
        $firstOption = $this->examples->firstOption($topic, $firstExample);

        return $this->render('example/index.html.twig', [
            'topicsNavigation' => $topics,
            'currentTopic' => $topic,
            'currentOption' => null,
            'description' => $this->examples->description($topic, $firstExample, $firstOption),
            'composer' => $this->examples->composer($topic, $firstExample, $firstOption),
            'code' => $this->examples->code($topic, $firstExample, $firstOption),
            'output' => $this->examples->output($topic, $firstExample, $firstOption),
        ]);
    }
}
