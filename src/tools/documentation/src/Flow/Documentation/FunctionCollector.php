<?php

declare(strict_types=1);

namespace Flow\Documentation;

use PhpParser\Node\Stmt\{Function_, Namespace_};
use PhpParser\{Node, NodeVisitorAbstract};

class FunctionCollector extends NodeVisitorAbstract
{
    /**
     * @var array<string>
     */
    public array $functions = [];

    private $currentNamespace = '';

    public function enterNode(Node $node) : void
    {
        if ($node instanceof Namespace_) {
            $this->currentNamespace = $node->name ? $node->name->toString() : '';
        }

        if ($node instanceof Function_) {
            $fullyQualifiedName = $this->currentNamespace ?
                $this->currentNamespace . '\\' . $node->name->toString() :
                $node->name->toString();
            $this->functions[] = $fullyQualifiedName;
        }
    }
}
