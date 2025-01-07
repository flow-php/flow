<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use PhpParser\Node;
use PhpParser\Node\Expr\{FuncCall, New_};
use PhpParser\Node\Name;
use Rector\Contract\Rector\ConfigurableRectorInterface;
use Rector\Rector\AbstractRector;
use Symplify\RuleDocGenerator\ValueObject\RuleDefinition;

final class NewToFunctionCallRector extends AbstractRector implements ConfigurableRectorInterface
{
    /**
     * @var array<NewObjectToFunction>
     */
    private array $configuration = [];

    public function configure(array $configuration) : void
    {
        $this->configuration = $configuration;
    }

    public function getFunctionName(string $className) : ?string
    {
        foreach ($this->configuration as $newObjectToFunction) {
            if ($newObjectToFunction->className === $className) {
                return $newObjectToFunction->functionName;
            }
        }

        return null;
    }

    /**
     * @return array<string>
     */
    public function getNodeTypes() : array
    {
        return [New_::class];
    }

    public function getRuleDefinition() : RuleDefinition
    {
        return new RuleDefinition('Replace object instantiation with function calls', [
            [
                new CodeSample(
                    <<<'CODE_SAMPLE'
'new Rows();',
'rows();'
CODE_SAMPLE
                ),
            ],
        ]);
    }

    /**
     * @param Node $node
     *
     * @return null|Node
     */
    public function refactor(Node $node) : ?Node
    {
        if (!$node instanceof New_) {
            return null;
        }

        $className = $this->getName($node->class);
        $functionName = $this->getFunctionName($className);

        if ($functionName === null) {
            return null;
        }

        return new FuncCall(new Name\FullyQualified($functionName), $node->getArgs());
    }
}
