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

    #[Route('/{topic}/{example}/', name: 'example', priority: -2)]
    public function example(string $topic, string $example) : Response
    {
        switch (\mb_strtolower($topic)) {
            case 'data_sink':
                return $this->redirectToRoute('example', ['topic' => 'data_writing', 'example' => $example], 301);
            case 'data_source':
                return $this->redirectToRoute('example', ['topic' => 'data_reading', 'example' => $example], 301);
            case 'data_frame':
                switch (\mb_strtolower($example)) {
                    case 'create_entries':
                        return $this->redirectToRoute('example', ['topic' => 'data_frame', 'example' => 'create_columns'], 301);
                    case 'rename_entries':
                        return $this->redirectToRoute('example', ['topic' => 'data_frame', 'example' => 'rename_columns'], 301);
                    case 'reorder_entries':
                        return $this->redirectToRoute('example', ['topic' => 'data_frame', 'example' => 'reorder_columns'], 301);
                }
        }

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

    #[Route('/{topic}/', name: 'topic', priority: -1)]
    public function topic(string $topic) : Response
    {
        switch (\mb_strtolower($topic)) {
            case 'data_sink':
                return $this->redirectToRoute('topic', ['topic' => 'data_writing'], 301);
            case 'data_source':
                return $this->redirectToRoute('topic', ['topic' => 'data_reading'], 301);
        }

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
