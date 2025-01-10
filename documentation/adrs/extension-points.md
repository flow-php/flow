# Extension Points

Proposed by: @norberttech  
Date: 2025-01-09

## Context
---

Flow is a framework, and as all frameworks, it should provide extension points for developers to hook into the framework and extend its functionality.

How to find an extension point? 

Easy, look for interfaces! For example take a look at `\Flow\ETL\Cache` which is 
an interface with few implementations. Thanks to the interface, you can easily
create your own implementation and use it in the framework.

Most of the extension points exposed through `\Flow\ETL\Config` where you can
replace a default implementations of various interfaces with those that better 
fit your needs.

Those extension points are there by design and should follow the backward compatibility policy. 

Are there any other extension points that we should consider?

Yes... Unfortunately, each class that is not marked final and that is being somehow
injected into the DataFrame must be considered as an extension point. 

Why? Because nothing prevents developers from extending those classes and injecting 
their custom version into the DataFrame.

Those extension points can't be covered by our backward compatibility policy. 

## Decision
---

**Closed by default**

All classes should be marked as `final` and whenever we need to expose an extension point,
we should do it through an interface.

**Always possible to open later**

In a justifiable cases, we can expose extension points through abstract classes or
simply allowing people to extend the class. Those must always be approved by one of
the core contributors. 

**No guarantee and support for workarounds**

Developers can still use reflections or dedicated tools to "unfinalize" Flow classes, 
but we do not guarantee backward compatibility for those cases and everyone should 
do it at their own risk. As a Flow PHP maintainers, we also reserve the right to
not provide any help or support for those cases.

## Pros & Cons
---

- zero risk of breaking accidentally backward compatibility promise
- more predictable behavior
- reduced cost of maintaining backward compatibility
- easier to find an actual extension points
- impossible to mock classes that aren’t marked as `final` in tests (which is a good thing, users shouldn’t mock Flow classes in their test suites)

## Alternatives Considered (optional)
---

Alternatively we could take similar approach to Symfony and mark classes as `final`
[when it makes sense](https://github.com/symfony/symfony/issues/15233#issuecomment-1566682054)
but this approach is too easy to get abused. 

Plus, changing non-final class into a final one is much bigger BC break
then the other way around.

## Links and References (optional)
---

- [When to declare classes final](https://ocramius.github.io/blog/when-to-declare-classes-final/)
- [Final Classes by default, why?](https://matthiasnoback.nl/2018/09/final-classes-by-default-why/)