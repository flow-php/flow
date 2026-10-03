/*
 * getcontext() / makecontext() / swapcontext() for Emscripten, built on its fiber API (needs -sASYNCIFY).
 *
 * PHP built with --disable-fiber-asm switches Fiber stacks through these three calls (Zend/zend_fibers.c), and
 * Emscripten's libc has none of them. Only what zend_fibers.c does is supported; anything else aborts with a message:
 *   - getcontext() on a context that makecontext() prepares next,
 *   - makecontext() with no arguments, uc_link == NULL and uc_stack set,
 *   - swapcontext() from the running context to a prepared or previously saved one. The main context is never
 *     passed to getcontext() (zend_fiber_init() points it at uninitialised memory), so the running context is
 *     tracked here, and the main one is captured at the first swap.
 */

#include <emscripten/fiber.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>

/*
 * At a swap, Asyncify spills the wasm locals of every frame on the stack into the context's buffer: about 1.2 bytes
 * per byte of C stack in use (measured on PHP 8.5: ~336 against ~276 bytes per nested internal callback).
 * A Fiber gives half of its own stack to the buffer, so nesting is bounded by the stack as on native PHP, at no extra
 * memory. The main context is captured at its first swap, from Flow's pipeline loop (~1.3 KB measured); 256 KB covers
 * ~750 nested internal callbacks there. Overflowing either buffer traps in Asyncify ("unreachable"), never corrupts.
 */
#define FLOW_UCONTEXT_MAIN_ASYNCIFY_SIZE (256 * 1024)
#define FLOW_UCONTEXT_MIN_STACK_SIZE (64 * 1024)

typedef struct flow_ucontext {
    emscripten_fiber_t fiber;
    void (*entry)(void);
    size_t asyncify_size;
    char *asyncify_stack;
} flow_ucontext;

_Static_assert(sizeof(((ucontext_t *) 0)->uc_mcontext) >= sizeof(flow_ucontext *), "uc_mcontext cannot hold the state pointer");

static flow_ucontext *running = NULL;
static flow_ucontext main_context;
static char main_asyncify_stack[FLOW_UCONTEXT_MAIN_ASYNCIFY_SIZE] __attribute__((aligned(16)));

static _Noreturn void refuse(const char *what)
{
    fprintf(stderr, "ucontext-emscripten: %s is not supported\n", what);
    abort();
}

static flow_ucontext **slot(ucontext_t *ucp)
{
    return (flow_ucontext **) &ucp->uc_mcontext;
}

#ifdef FLOW_UCONTEXT_TRACE
/* the context that swapped away last: until it is resumed, its buffer holds its whole unwound stack */
static flow_ucontext *unwound = NULL;
static size_t high_water_main = 0;
static size_t high_water_fiber = 0;

static void trace(void)
{
    size_t used = (size_t) ((char *) unwound->fiber.asyncify_data.stack_ptr - unwound->asyncify_stack);
    size_t *mark = unwound == &main_context ? &high_water_main : &high_water_fiber;

    if (used > *mark) {
        *mark = used;
        fprintf(
            stderr,
            "ucontext-emscripten: asyncify high water %s %zu bytes, C stack in use %zu bytes\n",
            unwound == &main_context ? "main" : "fiber",
            used,
            (size_t) ((char *) unwound->fiber.stack_base - (char *) unwound->fiber.stack_ptr)
        );
    }
}
#endif

static void enter(void *arg)
{
#ifdef FLOW_UCONTEXT_TRACE
    trace();
#endif

    ((flow_ucontext *) arg)->entry();

    refuse("returning from a makecontext() entry (uc_link is NULL)");
}

int getcontext(ucontext_t *ucp)
{
    *slot(ucp) = NULL;

    return 0;
}

void makecontext(ucontext_t *ucp, void (*func)(void), int argc, ...)
{
    if (argc != 0) {
        refuse("makecontext() with arguments");
    }

    if (ucp->uc_link != NULL) {
        refuse("makecontext() with uc_link");
    }

    char *bottom = (char *) ucp->uc_stack.ss_sp;
    size_t size = ucp->uc_stack.ss_size;

    if (bottom == NULL || size < FLOW_UCONTEXT_MIN_STACK_SIZE) {
        refuse("makecontext() without a uc_stack of at least 64 KiB");
    }

    /* the state and its Asyncify buffer take the top half of the stack, which grows down, and are freed with it */
    size_t asyncify_size = (size / 2 - sizeof(flow_ucontext) - 32) & ~(size_t) 15;
    flow_ucontext *context = (flow_ucontext *) (((uintptr_t) bottom + size - sizeof(flow_ucontext) - asyncify_size - 32) & ~(uintptr_t) 15);

    context->entry = func;
    context->asyncify_size = asyncify_size;
    context->asyncify_stack = (char *) (((uintptr_t) (context + 1) + 15) & ~(uintptr_t) 15);

    emscripten_fiber_init(&context->fiber, enter, context, bottom, (size_t) ((char *) context - bottom), context->asyncify_stack, context->asyncify_size);

    *slot(ucp) = context;
}

int swapcontext(ucontext_t *oucp, const ucontext_t *ucp)
{
    if (running == NULL) {
        main_context.asyncify_size = sizeof(main_asyncify_stack);
        main_context.asyncify_stack = main_asyncify_stack;
        emscripten_fiber_init_from_current_context(&main_context.fiber, main_asyncify_stack, sizeof(main_asyncify_stack));
        running = &main_context;
    }

    flow_ucontext *target = *slot((ucontext_t *) ucp);

    if (target == NULL) {
        refuse("swapcontext() to a context makecontext() did not prepare");
    }

    flow_ucontext *previous = running;

    *slot(oucp) = previous;
    running = target;

#ifdef FLOW_UCONTEXT_TRACE
    unwound = previous;
#endif

    emscripten_fiber_swap(&previous->fiber, &target->fiber);

#ifdef FLOW_UCONTEXT_TRACE
    trace();
#endif

    return 0;
}
