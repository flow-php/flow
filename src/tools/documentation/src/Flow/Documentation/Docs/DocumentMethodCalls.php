<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use PhpParser\Node;
use PhpParser\Node\Expr\Assign;
use PhpParser\Node\Expr\MethodCall;
use PhpParser\Node\Expr\NullsafeMethodCall;
use PhpParser\Node\Expr\StaticCall;
use PhpParser\Node\Expr\Variable;
use PhpParser\Node\Identifier;
use PhpParser\Node\Name;
use PhpParser\NodeFinder;
use PhpParser\NodeTraverser;
use PhpParser\NodeVisitor\NameResolver;
use PhpParser\Parser;
use PhpParser\ParserFactory;
use PhpParser\PhpVersion;
use Throwable;

use function class_exists;
use function interface_exists;
use function ltrim;
use function method_exists;
use function sprintf;
use function str_starts_with;

/**
 * Method calls a page makes on a receiver whose class is known, and that the class does not declare.
 *
 * Only a call whose receiver resolves to a real class produces a verdict. That is deliberate: asking
 * merely whether SOME class anywhere declares a method by that name cannot tell a rename from a
 * coincidence - `Cost::total()` was renamed to `totalCost()`, but an unrelated dependency also has a
 * `total()`, so a name-only check stays silent on a defect it exists to find.
 */
final readonly class DocumentMethodCalls
{
    private Parser $parser;

    public function __construct()
    {
        $this->parser = (new ParserFactory())->createForVersion(PhpVersion::fromComponents(8, 3));
    }

    /**
     * @param list<Fence> $fences
     *
     * @return array<string, list<string>> fence location => the calls that do not exist
     */
    public function unresolved(array $fences): array
    {
        $unresolved = [];

        $types = new InferredTypes();

        foreach ($fences as $fence) {
            $types->beginFence();
            $statements = $this->parse($fence);

            if ($statements === null) {
                continue;
            }

            $missing = [];
            $finder = new NodeFinder();

            // one statement at a time, calls before assignments: `$q = $q->where(...)` must judge
            // where() against what $q was, not against what the same line makes it
            foreach ($statements as $statement) {
                foreach ($finder->findInstanceOf($statement, Node::class) as $node) {
                    $call = $this->describe($node, $types);

                    if ($call !== null) {
                        $missing[] = $call;
                    }
                }

                foreach ($finder->findInstanceOf($statement, Assign::class) as $assign) {
                    if ($assign->var instanceof Variable) {
                        $types->remember($assign->var, $assign->expr);
                    }
                }
            }

            if ($missing !== []) {
                $unresolved[$fence->location()] = $missing;
            }
        }

        return $unresolved;
    }

    private function describe(Node $node, InferredTypes $types): ?string
    {
        if ($node instanceof MethodCall || $node instanceof NullsafeMethodCall) {
            $name = $node->name;

            if (!$name instanceof Identifier || !$types->establishedHere($node)) {
                return null;
            }

            return $this->missing($types->of($node->var), $name->toString());
        }

        if ($node instanceof StaticCall && $node->class instanceof Name && $node->name instanceof Identifier) {
            $class = ltrim($node->class->toString(), '\\');

            if (!class_exists($class) && !interface_exists($class)) {
                return null;
            }

            return $this->missing($class, $node->name->toString());
        }

        return null;
    }

    private function missing(?string $class, string $method): ?string
    {
        if ($class === null || method_exists($class, $method) || method_exists($class, '__call')) {
            return null;
        }

        return sprintf('%s::%s()', $class, $method);
    }

    /**
     * @return null|array<Node>
     */
    private function parse(Fence $fence): ?array
    {
        $code = $fence->code;

        if (!str_starts_with(ltrim($code), '<?php')) {
            $code = "<?php\n" . $code;
        }

        try {
            $statements = $this->parser->parse($code);

            // NameResolver throws on a duplicate import, which is itself a defect the fence test reports
            return $statements === null ? null : (new NodeTraverser(new NameResolver()))->traverse($statements);
        } catch (Throwable) {
            return null;
        }
    }
}
