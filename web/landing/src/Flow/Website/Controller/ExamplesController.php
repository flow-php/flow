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

    #[Route('/{topic}/{example}/', name: 'example')]
    public function example(string $topic, string $example) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = $example;

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $topic,
            'currentExample' => $example,
            'description' => $this->examples->description($currentTopic, $currentExample),
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }

    #[Route('/{topic}/', name: 'topic', priority: 10)]
    public function topic(string $topic) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = \current($examples);

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'currentTopic' => $currentTopic,
            'currentExample' => $currentExample,
            'description' => $this->examples->description($currentTopic, $currentExample),
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }
}
