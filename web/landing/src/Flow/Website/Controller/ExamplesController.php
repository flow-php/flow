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
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = $example;

        $options = $this->examples->options($currentTopic, $currentExample);

        if (\count($options) > 0) {
            $firstOption = \current($options);

            return $this->redirectToRoute('example_option', [
                'topic' => $topic,
                'example' => $example,
                'option' => $firstOption,
            ]);
        }

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'options' => [],
            'currentTopic' => $topic,
            'currentExample' => $example,
            'currentOption' => null,
            'description' => $this->examples->description($currentTopic, $currentExample),
            'composer' => $this->examples->composer($currentTopic, $currentExample),
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }

    #[Route('/{topic}/{example}/{option}/', name: 'example_option', priority: -150)]
    public function exampleOption(string $topic, string $example, string $option) : Response
    {
        $topics = $this->examples->topics();
        $currentTopic = $topic;

        $examples = $this->examples->examples($currentTopic);
        $currentExample = $example;

        $options = $this->examples->options($currentTopic, $currentExample);
        $currentOption = $option;

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'options' => $options,
            'currentTopic' => $topic,
            'currentExample' => $example,
            'currentOption' => $currentOption,
            'description' => $this->examples->description($currentTopic, $currentExample, $currentOption),
            'composer' => $this->examples->composer($currentTopic, $currentExample, $currentOption),
            'code' => $this->examples->code($currentTopic, $currentExample, $currentOption),
            'output' => $this->examples->output($currentTopic, $currentExample, $currentOption),
        ]);
    }

    #[Route('/{topic}/', name: 'topic', priority: -120)]
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

        $options = $this->examples->options($currentTopic, $currentExample);

        if (\count($options) > 0) {
            $firstOption = \current($options);

            return $this->redirectToRoute('example_option', [
                'topic' => $currentTopic,
                'example' => $currentExample,
                'option' => $firstOption,
            ]);
        }

        return $this->render('example/index.html.twig', [
            'topics' => $topics,
            'examples' => $examples,
            'options' => [],
            'currentTopic' => $currentTopic,
            'currentExample' => $currentExample,
            'currentOption' => null,
            'description' => $this->examples->description($currentTopic, $currentExample),
            'composer' => $this->examples->composer($currentTopic, $currentExample),
            'code' => $this->examples->code($currentTopic, $currentExample),
            'output' => $this->examples->output($currentTopic, $currentExample),
        ]);
    }
}
